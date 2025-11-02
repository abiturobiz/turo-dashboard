#!/usr/bin/env python3
"""
download_turo_csv.py

- Navigates to Turo Host Earnings
- Robustly finds & clicks the CSV/Export control
- Saves CSV to data/turo_csv/
- Runs ETL (etl_turo_earnings.py) to update turo.duckdb

Usage:
  Local (interactive profile):
    python download_turo_csv.py
    # or headless:
    python download_turo_csv.py --headless

  CI (GitHub Actions):
    export AUTH_STORAGE_STATE=auth/storage_state.json
    python download_turo_csv.py --headless
"""

import os
import re
import time
import argparse
import subprocess
from pathlib import Path
from datetime import datetime

from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PWTimeout,
    Page,  # type hints
)

# ----------------------------
# Paths / constants
# ----------------------------
CSV_DIR = Path("data/turo_csv")
OUT_DIR = Path("out")
USER_DATA_DIR = Path(".pw-user")  # local persistent chromium profile
CSV_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

ETL_CMD = ["python", "etl_turo_earnings.py", "--csv_dir", str(CSV_DIR), "--db", "turo.duckdb"]
EARNINGS_URL = "https://turo.com/host/earnings"


# ----------------------------
# Utilities
# ----------------------------
def ensure_dirs() -> None:
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def log(msg: str) -> None:
    print(f"[download] {msg}", flush=True)


def wait_for_manual_login(page: Page) -> None:
    """
    LOCAL use only: let the user finish interactive login/MFA,
    then press ENTER in terminal to continue.
    """
    page.bring_to_front()
    log("\n==> Please complete Turo login in the opened browser window.")
    log("    After you SEE the Earnings page, return here and press ENTER.")
    input("==> Press ENTER to continue... ")


def go_to_earnings(page: Page) -> None:
    """
    Navigate (or re-navigate) to the Earnings page and wait for signals that it rendered.
    """
    page.goto(EARNINGS_URL, wait_until="domcontentloaded")
    try:
        page.get_by_role("heading", name=re.compile(r"earning|payout|trip", re.I)).first.wait_for(timeout=10_000)
    except Exception:
        try:
            page.get_by_text(re.compile(r"earning|payout|export|csv|trips|month", re.I)).first.wait_for(timeout=10_000)
        except Exception:
            try:
                page.wait_for_load_state("networkidle", timeout=10_000)
            except PWTimeout:
                pass


def _close_banners(scope: Page) -> None:
    """Dismiss cookie/consent banners or toasts that may cover controls."""
    for txt in [
        r"Accept( all)? cookies",
        r"\bAccept\b",
        r"Got it",
        r"I understand",
        r"Dismiss",
        r"Close",
    ]:
        try:
            scope.get_by_role("button", name=re.compile(txt, re.I)).first.click(timeout=1500)
        except Exception:
            pass
    try:
        scope.get_by_text(re.compile(r"accept|agree", re.I)).first.click(timeout=1000)
    except Exception:
        pass


def _candidate_locators(scope: Page):
    """
    Build a list of locator factories to try for CSV/Export.
    Each returns a Locator when called.
    """
    locs = []
    # Common visible names
    for label in [
        r"\bCSV\b",
        r"Download CSV",
        r"Export CSV",
        r"Export .*CSV",
        r"\bExport\b",
        r"\bDownload\b",
        r"Download report",
    ]:
        locs.append(lambda s=scope, l=label: s.get_by_role("button", name=re.compile(l, re.I)))
        locs.append(lambda s=scope, l=label: s.get_by_role("link",   name=re.compile(l, re.I)))
        locs.append(lambda s=scope, l=label: s.get_by_text(re.compile(l, re.I)))

    # Attributes / CSS heuristics
    for sel in [
        "[data-testid*=csv i]",
        "[data-testid*=export i]",
        "[data-qa*=csv i]",
        "[aria-label*=csv i]",
        "[aria-label*=export i]",
        "button:has-text('CSV')",
        "button:has-text('Export')",
        "a:has-text('CSV')",
        "a:has-text('Export')",
    ]:
        locs.append(lambda s=scope, css=sel: s.locator(css))

    # Direct anchors to csv
    for sel in [
        "a[download$='.csv']",
        "a[href$='.csv']",
        "a[href*='.csv?']",
    ]:
        locs.append(lambda s=scope, css=sel: s.locator(css))

    return locs


def _find_and_click_csv(scope: Page) -> bool:
    """
    Try to click a CSV/Export control within this scope.
    Returns True if a click was performed that should trigger a download.
    """
    _close_banners(scope)

    # Direct: buttons/links/anchors that are visible
    for make in _candidate_locators(scope):
        try:
            loc = make()
            if loc and loc.count() > 0:
                # Prefer visible
                for i in range(min(loc.count(), 6)):
                    el = loc.nth(i)
                    if el.is_visible():
                        el.click()
                        return True
        except Exception:
            continue

    # Export menu path: click Export, then CSV inside the menu
    try:
        export_btn = scope.get_by_role("button", name=re.compile(r"\bExport\b", re.I)).first
        if export_btn and export_btn.is_visible():
            export_btn.click(timeout=2000)
            time.sleep(0.3)
            for txt in [r"\bCSV\b", r"Export CSV", r"Download CSV"]:
                try:
                    scope.get_by_role("menuitem", name=re.compile(txt, re.I)).first.click(timeout=1500)
                    return True
                except Exception:
                    pass
            # generic text as a fallback
            try:
                scope.get_by_text(re.compile(r"\bCSV\b", re.I)).first.click(timeout=1500)
                return True
            except Exception:
                pass
    except Exception:
        pass

    # Kebab / overflow menu (three dots), then Export -> CSV
    try:
        kebab = scope.locator("button:has(svg)").filter(has_text=re.compile(r"⋮|more|overflow", re.I)).first
        if kebab and kebab.is_visible():
            kebab.click(timeout=1500)
            time.sleep(0.2)
            for txt in [r"\bExport\b", r"\bDownload\b"]:
                try:
                    scope.get_by_role("menuitem", name=re.compile(txt, re.I)).first.click(timeout=1500)
                    # then CSV inside submenu
                    for t2 in [r"\bCSV\b", r"Export CSV", r"Download CSV"]:
                        try:
                            scope.get_by_text(re.compile(t2, re.I)).first.click(timeout=1500)
                            return True
                        except Exception:
                            pass
                except Exception:
                    pass  # continue outer for loop
            # fallback if still nothing clicked
            try:
                scope.get_by_text(re.compile(r"\bCSV\b", re.I)).first.click(timeout=1500)
                return True
            except Exception:
                pass
    except Exception:
        pass

    return False


def click_download_and_save(page: Page) -> Path:
    """
    Trigger CSV download and save to data/turo_csv/.
    Searches main page and iframes. Dumps debug artifacts if not found.
    """
    # Let the page settle
    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
    except PWTimeout:
        pass

    # Try main document first
    if _find_and_click_csv(page):
        with page.expect_download(timeout=30_000) as dl:
            pass
        download = dl.value
        stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        target = CSV_DIR / f"turo_earnings_{stamp}.csv"
        download.save_as(str(target))
        log(f"Downloaded CSV -> {target}")
        return target

    # Try child iframes
    for f in page.frames:
        if f == page.main_frame:
            continue
        try:
            # Skip obvious ad/analytics frames
            if any(x in (f.url or "") for x in ["ads", "doubleclick", "googletag", "tracking"]):
                continue
            if _find_and_click_csv(f):
                with page.expect_download(timeout=30_000) as dl:
                    pass
                download = dl.value
                stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
                target = CSV_DIR / f"turo_earnings_{stamp}.csv"
                download.save_as(str(target))
                log(f"Downloaded CSV via iframe -> {target}")
                return target
        except Exception:
            continue

    # If we reach here, we failed; dump artifacts
    html_path = OUT_DIR / "debug_earnings_page.html"
    png_path = OUT_DIR / "debug_earnings_page.png"
    try:
        html = page.content()
        html_path.write_text(html, encoding="utf-8")
        page.screenshot(path=str(png_path), full_page=True)
        log(f"[debug] Could not find a download control. Saved HTML: {html_path}, screenshot: {png_path}")
        log(f"[debug] Current URL: {page.url}")
    except Exception:
        pass

    raise RuntimeError("Could not find the 'Download CSV' control. Inspect debug artifacts and adjust selectors.")


# ----------------------------
# Main flow
# ----------------------------
def main(headless: bool):
    ensure_dirs()
    storage_state_path = os.environ.get("AUTH_STORAGE_STATE", "").strip()

    with sync_playwright() as p:
        if storage_state_path and Path(storage_state_path).exists():
            # CI path: fresh context from storage_state
            log(f"Using storage_state: {storage_state_path}")
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(storage_state=storage_state_path, accept_downloads=True)
            page = context.new_page()
        else:
            # LOCAL path: persistent profile (keeps you logged in)
            log("Using persistent local profile (.pw-user)")
            browser = p.chromium.launch_persistent_context(
                user_data_dir=str(USER_DATA_DIR),
                headless=headless,
                accept_downloads=True,
            )
            page = browser.new_page()

        # Apply a generous timeout on the PAGE (not on Browser)
        page.set_default_timeout(5 * 60 * 1000)

        try:
            go_to_earnings(page)

            # Local non-headless: if not seeing export quickly, allow interactive login
            if not storage_state_path and not headless:
                try:
                    page.get_by_text(re.compile(r"export|csv", re.I)).first.wait_for(timeout=3000)
                except Exception:
                    wait_for_manual_login(page)
                    go_to_earnings(page)

            # Attempt the download
            _ = click_download_and_save(page)

        finally:
            try:
                if storage_state_path:
                    context.close()
                    browser.close()
                else:
                    browser.close()
            except Exception:
                pass

    # Run ETL after successful download
    log(f"Running ETL: {' '.join(ETL_CMD)}")
    subprocess.run(ETL_CMD, check=True)
    log("ETL complete.")


# ----------------------------
# CLI
# ----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download Turo earnings CSV and run ETL.")
    parser.add_argument("--headless", action="store_true", help="Run in headless mode (CI friendly).")
    args = parser.parse_args()
    main(headless=args.headless)
