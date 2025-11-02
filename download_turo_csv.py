# download_turo_csv.py
import re
import os
import time, argparse, subprocess
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

USER_DATA_DIR = Path("./.pw-profile")     # saved cookies/session
DOWNLOAD_DIR  = Path("./data/turo_csv")
ETL_CMD       = ["python", "etl_turo_earnings.py", "--csv_dir", str(DOWNLOAD_DIR), "--db", "turo.duckdb"]

LOGIN_WAIT_MS = 10 * 60 * 1000  # 10 minutes for first-time login/MFA
CLICK_WAIT_MS = 60 * 1000       # 60s to find/press Download button
DL_WAIT_MS    = 5 * 60 * 1000   # 5 minutes to complete download

def ensure_dirs():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    USER_DATA_DIR.mkdir(parents=True, exist_ok=True)

def go_to_earnings(page):
    # Go directly; if redirected to login, that's fine.
    page.goto("https://turo.com/host/earnings", wait_until="domcontentloaded")

def wait_for_manual_login(page):
    # Give you plenty of time to complete sign-in/MFA.
    print("==> Take your time to log in / complete MFA in the opened browser.")
    print("==> Navigate until you SEE the Earnings page with the Download CSV control.")
    print("==> When ready, come back here and press ENTER to continue.")
    try:
        # Keep browser alive while you log in
        deadline = time.time() + LOGIN_WAIT_MS / 1000.0
        while time.time() < deadline:
            time.sleep(3)
            if page.is_closed():
                raise SystemExit("Browser window was closed. Re-run the script.")
            # If you like, you can try to auto-detect being on the earnings page:
            # Break early if we see the button text in DOM (best-effort)
            if page.locator("text=CSV").first.is_visible(timeout=500):
                break
        input()  # final confirmation from you
    except PWTimeout:
        pass  # We'll rely on your ENTER press

def click_download_and_save(page):
    """
    Find and click the 'Download CSV' control using several robust locators.
    Saves the downloaded file to data/turo_csv/ and returns the path.
    """
    # Try a few likely selectors in order of preference
    candidate_locators = [
        # Accessible roles (button or link) with name containing CSV
        page.get_by_role("button", name=re.compile(r"download\s*csv", re.I)),
        page.get_by_role("link",   name=re.compile(r"download\s*csv", re.I)),

        # Generic "CSV" fallback (sometimes the control just says CSV)
        page.get_by_role("button", name=re.compile(r"csv", re.I)),
        page.get_by_role("link",   name=re.compile(r"csv", re.I)),

        # Text-based locator
        page.get_by_text(re.compile(r"download\s*csv", re.I)),

        # CSS/XPath fallbacks (adjust if you discover a stable attribute)
        page.locator("button:has-text('CSV')"),
        page.locator("a:has-text('CSV')"),

        # Absolute last resort: any anchor that looks like a CSV download
        page.locator("a[href$='.csv']"),
    ]

    last_error = None
    for loc in candidate_locators:
        try:
            loc.wait_for(state="visible", timeout=60_000)  # wait up to 60s
            with page.expect_download(timeout=300_000) as dl_info:  # 5 min
                loc.click()
            download = dl_info.value
            ts = time.strftime("%Y%m%d-%H%M%S")
            out_path = DOWNLOAD_DIR / f"turo_earnings_{ts}.csv"
            download.save_as(out_path)
            print(f"Saved: {out_path}")
            return out_path
        except Exception as e:
            last_error = e
            continue

    # If we get here, none of the locators worked—dump a screenshot for debugging.
    debug_png = Path("out") / "debug_earnings_page.png"
    debug_png.parent.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(debug_png), full_page=True)
    print(f"[debug] Could not find a download control. Saved a screenshot at: {debug_png}")
    print(f"[debug] Current URL: {page.url}")
    raise SystemExit(
        "Could not find the 'Download CSV' button/link. "
        "Inspect the screenshot and, if needed, update the selector to a stable attribute."
    ) from last_error


def main(headless: bool):
    ensure_dirs()
    storage_state_path = os.environ.get("AUTH_STORAGE_STATE", "").strip()

    with sync_playwright() as p:
        if storage_state_path and Path(storage_state_path).exists():
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(storage_state=storage_state_path, accept_downloads=True)
            page = context.new_page()
        else:
            browser = p.chromium.launch_persistent_context(
                user_data_dir=str(USER_DATA_DIR),
                headless=headless,
                accept_downloads=True,
            )
            page = browser.new_page()

        # ✅ Correct: apply timeout on the PAGE, not the BROWSER
        page.set_default_timeout(5 * 60 * 1000)

        try:
            go_to_earnings(page)
            try:
                page.get_by_text(re.compile(r"csv", re.I)).first.wait_for(state="visible", timeout=3000)
            except Exception:
                if not storage_state_path:
                    wait_for_manual_login(page)
            go_to_earnings(page)
            csv_path = click_download_and_save(page)
        finally:
            if storage_state_path:
                context.close()
                browser.close()
            else:
                browser.close()

    subprocess.run(ETL_CMD, check=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--headless", action="store_true", help="Run Chromium headless (use after first login is saved)")
    args = ap.parse_args()
    main(headless=args.headless)
