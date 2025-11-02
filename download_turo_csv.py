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

def click_download_and_save(page: Page) -> Path:
    """
    Attempts to trigger the CSV download and save to data/turo_csv/.
    Searches main page and iframes. Dumps debug artifacts if not found.
    """
    # Wait for page to be stable-ish
    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except PWTimeout:
        pass

    # Try main page first
    if _find_and_click_csv(page):
        with page.expect_download(timeout=30000) as dl:
            # if click already happened, the download event should fire shortly
            pass
        download = dl.value
        stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        target = CSV_DIR / f"turo_earnings_{stamp}.csv"
        download.save_as(str(target))
        return target

    # Try inside iframes (some apps render the earnings table in an iframe)
    for f in page.frames:
        if f == page.main_frame:
            continue
        try:
            # quick heuristic to skip ad/analytics frames
            if any(x in (f.url or "") for x in ["ads", "doubleclick", "googletag"]):
                continue
            if _find_and_click_csv(f):
                with page.expect_download(timeout=30000) as dl:
                    pass
                download = dl.value
                stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
                target = CSV_DIR / f"turo_earnings_{stamp}.csv"
                download.save_as(str(target))
                return target
        except Exception:
            continue

    # Nothing worked — dump artifacts and raise
    html_path = DEBUG_DIR / "debug_earnings_page.html"
    png_path  = DEBUG_DIR / "debug_earnings_page.png"
    try:
        html = page.content()
        html_path.write_text(html, encoding="utf-8")
        page.screenshot(path=str(png_path), full_page=True)
    except Exception:
        pass

    cur_url = page.url
    raise RuntimeError(
        "Could not find the 'Download CSV' control. "
        f"Saved debug HTML at {html_path}, screenshot at {png_path}. Current URL: {cur_url}"
    )


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
