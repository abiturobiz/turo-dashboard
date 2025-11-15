#!/usr/bin/env python3
"""
download_turo_csv.py

CI-friendly Playwright script that:
1) Opens Turo Business → Earnings (current year)
2) Clicks the visible "Download CSV" button
3) Saves CSV into data/turo_csv/
4) Runs the ETL: etl_turo_earnings.py --csv_dir data/turo_csv --db turo.duckdb
"""

import os
import re
import csv
import time
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
from typing import List, Optional

from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PWTimeout,
    Page,
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

def earnings_url_for(year: int) -> str:
    # This is the page shown in your screenshots with a visible "Download CSV" button
    return f"https://turo.com/us/en/business/earnings?year={year}"

# ----------------------------
# Small helpers
# ----------------------------
def log(msg: str) -> None:
    print(f"[download] {msg}", flush=True)

def ensure_dirs() -> None:
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

def _close_overlays(page: Page) -> None:
    """Close cookie banners / chat bubbles that can block clicks."""
    # Obvious buttons
    for txt in [
        r"Accept( all)? cookies",
        r"\bAccept\b",
        r"Got it",
        r"I understand",
        r"Dismiss",
        r"Close chat",
        r"Close",
    ]:
        try:
            page.get_by_role("button", name=re.compile(txt, re.I)).first.click(timeout=1200)
        except Exception:
            pass
    # Sometimes there are generic text-only banners
    try:
        page.get_by_text(re.compile(r"accept|agree", re.I)).first.click(timeout=1000)
    except Exception:
        pass
    # Remove known chat DOMs
    try:
        page.evaluate("""
(() => {
  const killers = [
    'iframe[src*="intercom"]',
    'iframe[src*="helpshift"]',
    'iframe[src*="zendesk"]',
    '.intercom-lightweight-app',
    '[data-testid*="chat"]',
    '[class*="chat"]'
  ];
  killers.forEach(sel => document.querySelectorAll(sel).forEach(n => n.remove()));
})();
""")
    except Exception:
        pass

def _dump_debug(page: Page, name_prefix: str) -> None:
    try:
        html_path = OUT_DIR / f"{name_prefix}.html"
        png_path = OUT_DIR / f"{name_prefix}.png"
        html_path.write_text(page.content(), encoding="utf-8")
        page.screenshot(path=str(png_path), full_page=True)
        log(f"[debug] Saved HTML: {html_path}, screenshot: {png_path}")
        log(f"[debug] Current URL: {page.url}")
    except Exception:
        pass

def _visible_texts(loc) -> List[str]:
    out = []
    try:
        n = min(loc.count(), 50)
        for i in range(n):
            el = loc.nth(i)
            try:
                if el.is_visible():
                    t = el.inner_text(timeout=1000).strip()
                    if t:
                        out.append(t.replace("\n", " ")[:200])
            except Exception:
                pass
    except Exception:
        pass
    return out

def _dump_controls(page: Page, tag: str = "main") -> None:
    try:
        btns = page.locator("button")
        links = page.locator("a")
        for t in _visible_texts(btns):
            print(f"[debug] {tag} [btn]  {t}")
        for t in _visible_texts(links):
            print(f"[debug] {tag} [link] {t}")
    except Exception:
        pass
def safe_goto(page: Page, url: str, delay_ms: int = 4000):
    page.goto(url, wait_until="domcontentloaded")
    page.wait_for_timeout(delay_ms)  # ⏸ small settle time (4s)
    try:
        page.wait_for_load_state("networkidle", timeout=10_000)
    except PWTimeout:
        pass

# ----------------------------
# Navigation to Business → Earnings
# ----------------------------
def go_to_business_earnings(page: Page, year: int) -> None:
    url = earnings_url_for(year)
    safe_goto(page, url, delay_ms=4000)

    # If we ever get bounced to login, fail early
    if re.search(r"/login|/auth", page.url or "", re.I):
        _dump_debug(page, "login_redirect")
        raise RuntimeError("Session cookie invalid: redirected to login. Recreate storage_state.json and update TURO_STORAGE_STATE_B64.")


def switch_to_host_earnings(page: Page):
    """If on business earnings, switch to host earnings."""
    if "business/earnings" in (page.url or ""):
        log("Detected Business Earnings page. Switching to Host Earnings...")
        host_url = "https://turo.com/host/earnings"
        safe_goto(page, host_url, delay_ms=2000)
        try:
            page.wait_for_url(lambda u: "host/earnings" in (u or ""), timeout=10_000)
        except Exception:
            pass
        page.wait_for_timeout(1000)  # extra 1s for React to bind handlers
        log("Switched to Host Earnings.")

# ----------------------------
# Click the "Download CSV" and save the file
# ----------------------------
def click_download_and_save(page: Page) -> Path:
    _close_overlays(page)
    page.wait_for_timeout(800)  # give the UI a beat to render the button


    # Make sure the control is in view
    try:
        page.mouse.wheel(0, 2000)
        page.wait_for_timeout(300)
    except Exception:
        pass

    candidates = [
        page.get_by_role("button", name=re.compile(r"^\s*Download\s+CSV\s*$", re.I)).first,
        page.get_by_text(re.compile(r"^\s*Download\s+CSV\s*$", re.I)).first,
        page.locator("button:has-text('Download CSV')").first,
        page.locator("a:has-text('Download CSV')").first,
        page.locator("xpath=//*[self::button or self::a or self::*[@role='button']][contains(normalize-space(.), 'Download CSV')]").first,
    ]

    # Try Playwright click with download expectation
    for loc in candidates:
        try:
            if loc and loc.count() > 0:
                loc.scroll_into_view_if_needed(timeout=2000)
                with page.expect_download(timeout=30_000) as dl_info:
                    loc.click(timeout=5000, force=True)
                d = dl_info.value
                stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
                target = CSV_DIR / f"turo_earnings_{stamp}.csv"
                d.save_as(str(target))
                log(f"Downloaded CSV -> {target}")
                return target
        except Exception:
            # Small nudge scrolls between attempts
            try:
                page.mouse.wheel(0, 800)
                page.wait_for_timeout(200)
            except Exception:
                pass

    # JS fallback: query & click
    try:
        handle = page.evaluate_handle("""
() => {
  const needle = /\\bdownload\\s*csv\\b/i;
  const nodes = Array.from(document.querySelectorAll('button, a, [role="button"]'));
  for (const n of nodes) {
    const t = (n.innerText || n.textContent || '').trim();
    if (needle.test(t)) return n;
  }
  return null;
}
""")
        if handle and handle.as_element():
            page.evaluate("(n) => n.scrollIntoView({behavior:'instant', block:'center'})", handle)
            with page.expect_download(timeout=30_000) as dl_info:
                page.evaluate("(n) => n.click()", handle)
            d = dl_info.value
            stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
            target = CSV_DIR / f"turo_earnings_{stamp}.csv"
            d.save_as(str(target))
            log(f"Downloaded CSV (JS fallback) -> {target}")
            return target
    except Exception:
        pass

    # If we got here, we didn’t find/click the control. Dump artifacts for inspection.
    _dump_debug(page, "debug_earnings_page")
    _dump_controls(page, "main")
    raise RuntimeError("Could not click the 'Download CSV' button on Business → Earnings. See debug artifacts in out/")

# ----------------------------
# Main
# ----------------------------
def main(headless: bool):
    ensure_dirs()

    storage_state_path = os.environ.get("AUTH_STORAGE_STATE", "").strip()
    is_ci = os.environ.get("GITHUB_ACTIONS") == "true"
    # Allow env to override headless
    env_headless = os.environ.get("PLAYWRIGHT_HEADLESS")
    if env_headless is not None:
        headless_flag = env_headless.strip().lower() not in ("0", "false", "no")
    else:
        headless_flag = (False if is_ci else headless)

    launch_args = ["--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage", "--no-sandbox"]

    with sync_playwright() as p:
        # Use cookie-based context in CI; persistent profile locally
        if storage_state_path and Path(storage_state_path).exists():
            log(f"Using storage_state: {storage_state_path}")
            browser = p.chromium.launch(headless=headless_flag, args=launch_args)
            context = browser.new_context(
                storage_state=storage_state_path,
                accept_downloads=True,
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 900},
                locale="en-US",
                timezone_id="America/New_York",
            )
            # Light stealth
            context.add_init_script("""
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
Object.defineProperty(navigator, 'platform', { get: () => 'MacIntel' });
Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
""")
            page = context.new_page()
        else:
            log("Using persistent local profile (.pw-user)")
            browser = p.chromium.launch_persistent_context(
                user_data_dir=str(USER_DATA_DIR),
                headless=headless_flag,
                accept_downloads=True,
                args=launch_args,
            )
            browser.add_init_script("""
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
Object.defineProperty(navigator, 'platform', { get: () => 'MacIntel' });
Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
""")
            page = browser.new_page()

        # generous per-action timeout
        page.set_default_timeout(5 * 60 * 1000)

        try:
            # 1) Navigate straight to Business → Earnings (this is where "Download CSV" lives)
            year = datetime.utcnow().year
            go_to_business_earnings(page, year)
            EARNINGS_URL = "https://turo.com/host/earnings"
            switch_to_host_earnings(page)

            # 2) Try to click the button and save the CSV
            _ = click_download_and_save(page)

        finally:
            try:
                # close contexts/browsers
                if storage_state_path and Path(storage_state_path).exists():
                    context.close()
                    browser.close()
                else:
                    browser.close()
            except Exception:
                pass

    # 3) Run ETL
    log(f"Running ETL: {' '.join(ETL_CMD)}")
    subprocess.run(ETL_CMD, check=True)
    log("ETL complete.")

# ----------------------------
# CLI
# ----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download Turo earnings CSV and run ETL.")
    parser.add_argument("--headless", action="store_true", help="Run in headless mode")
    args = parser.parse_args()
    main(headless=args.headless)
