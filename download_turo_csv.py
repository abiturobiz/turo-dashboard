#!/usr/bin/env python3
"""
download_turo_csv.py

Automates download of Turo Host Earnings CSV and runs ETL to update turo.duckdb.

Features:
- Works in CI (GitHub Actions) using AUTH_STORAGE_STATE cookie (no MFA each run)
- Local persistent profile for manual login if needed
- Stealth/anti-bot signals, headful-friendly
- Robust multi-selector CSV click (buttons, links, menus, iframes)
- Network sniff fallback for CSV responses
- Table-scrape fallback to keep pipeline green when export UI is hidden
- Debug artifacts (HTML + screenshot + control dumps)

Env (CI):
  AUTH_STORAGE_STATE=auth/storage_state.json
  PLAYWRIGHT_HEADLESS=0 (recommended in workflow)

Usage:
  Local:
    python download_turo_csv.py
    python download_turo_csv.py --headless
  CI:
    python download_turo_csv.py --headless   # or headful via PLAYWRIGHT_HEADLESS=0
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
DOWNLOAD_DIR = Path("data/turo_csv")
CSV_DIR = Path("data/turo_csv")
OUT_DIR = Path("out")
USER_DATA_DIR = Path(".pw-user")  # local persistent chromium profile
CSV_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

ETL_CMD = ["python", "etl_turo_earnings.py", "--csv_dir", str(CSV_DIR), "--db", "turo.duckdb"]
EARNINGS_URL = "https://turo.com/us/en/business/earnings?year=2025"


# CSV capture via network sniff
csv_bytes = {"buf": None}

def _close_chat_and_overlays(page: Page):
    # Close obvious chat / overlays that can block clicks
    for txt in [r"Close chat", r"Dismiss", r"Got it", r"Accept( all)? cookies", r"\bAccept\b"]:
        try:
            page.get_by_role("button", name=re.compile(txt, re.I)).first.click(timeout=1200)
        except Exception:
            pass
    # Nuke common chat iframes/bubbles
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


# ----------------------------
# Utilities / logging
# ----------------------------
def ensure_dirs() -> None:
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def log(msg: str) -> None:
    print(f"[download] {msg}", flush=True)


# ----------------------------
# Navigation helpers
# ----------------------------
def wait_for_manual_login(page: Page) -> None:
    page.bring_to_front()
    log("\n==> Please complete Turo login/MFA in the opened browser window.")
    log("    After you SEE the Earnings page, return here and press ENTER.")
    input("==> Press ENTER to continue... ")


def go_to_earnings(page: Page) -> None:
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


def maybe_click_default_tab(page: Page) -> None:
    """If page is a hub with tabs, try typical tabs to reveal export controls."""
    for tab in ["Transactions", "Trips", "Reports", "Payouts", "Earnings"]:
        try:
            page.get_by_role("tab", name=re.compile(tab, re.I)).first.click(timeout=1500)
            time.sleep(0.3)
            break
        except Exception:
            pass


# ----------------------------
# UI handling & selectors
# ----------------------------
def _close_banners(scope: Page) -> None:
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
    """Return locator factories to probe for CSV/Export controls."""
    locs = []
    # Names
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

    # Attributes / CSS
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
        locs.append(lambda s=scope, css=sel: s.locator(sel))

    # Direct anchors to CSV
    for sel in [
        "a[download$='.csv']",
        "a[href$='.csv']",
        "a[href*='.csv?']",
    ]:
        locs.append(lambda s=scope, css=sel: s.locator(sel))

    return locs


def _find_and_click_csv(scope: Page) -> bool:
    _close_banners(scope)

    # Direct: visible controls
    for make in _candidate_locators(scope):
        try:
            loc = make()
            if loc and loc.count() > 0:
                for i in range(min(loc.count(), 8)):
                    el = loc.nth(i)
                    try:
                        if el.is_visible():
                            el.click()
                            return True
                    except Exception:
                        pass
        except Exception:
            continue

    # Export menu path
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
            try:
                scope.get_by_text(re.compile(r"\bCSV\b", re.I)).first.click(timeout=1500)
                return True
            except Exception:
                pass
    except Exception:
        pass

    # Kebab/overflow → Export → CSV
    try:
        kebab = scope.locator("button:has(svg)").filter(has_text=re.compile(r"⋮|more|overflow", re.I)).first
        if kebab and kebab.is_visible():
            kebab.click(timeout=1500)
            time.sleep(0.2)
            for txt in [r"\bExport\b", r"\bDownload\b"]:
                try:
                    scope.get_by_role("menuitem", name=re.compile(txt, re.I)).first.click(timeout=1500)
                    for t2 in [r"\bCSV\b", r"Export CSV", r"Download CSV"]:
                        try:
                            scope.get_by_text(re.compile(t2, re.I)).first.click(timeout=1500)
                            return True
                        except Exception:
                            pass
                except Exception:
                    pass
            try:
                scope.get_by_text(re.compile(r"\bCSV\b", re.I)).first.click(timeout=1500)
                return True
            except Exception:
                pass
    except Exception:
        pass

    return False


# ----------------------------
# Debug helpers
# ----------------------------
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


def dump_controls(scope, tag: str):
    btns = scope.locator("button")
    links = scope.locator("a")
    btn_texts = _visible_texts(btns)
    link_texts = _visible_texts(links)
    print(f"[debug] {tag} visible buttons (<=50):")
    for t in btn_texts:
        print("[debug]   [btn] ", t)
    print(f"[debug] {tag} visible links (<=50):")
    for t in link_texts:
        print("[debug]   [link]", t)
    for sel in ["a[download$='.csv']", "a[href$='.csv']", "a[href*='.csv?']"]:
        try:
            cnt = scope.locator(sel).count()
            print(f"[debug] {tag} selector '{sel}' count={cnt}")
        except Exception:
            pass


# ----------------------------
# Fallback: scrape a visible table
# ----------------------------
def _extract_table_to_csv(scope: Page, hint: str) -> Optional[Path]:
    candidates = [
        scope.get_by_role("table"),
        scope.locator("table"),
        scope.locator("[role='grid']"),
        scope.locator("[data-testid*=table i]"),
    ]
    for cand in candidates:
        try:
            if cand.count() == 0:
                continue
            target = None
            for i in range(min(cand.count(), 5)):
                el = cand.nth(i)
                if el.is_visible():
                    target = el
                    break
            if not target:
                continue

            html = target.inner_html(timeout=3000)

            # Headers
            hdr = re.findall(r"<th[^>]*>(.*?)</th>", html, flags=re.I | re.S)
            hdr = [re.sub("<[^<]+?>", " ", h).strip() for h in hdr]
            hdr = [re.sub(r"\s+", " ", h) for h in hdr]

            # Rows
            rows_html = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.I | re.S)
            rows = []
            for rh in rows_html:
                cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", rh, flags=re.I | re.S)
                cells = [re.sub("<[^<]+?>", " ", c).strip() for c in cells]
                cells = [re.sub(r"\s+", " ", c) for c in cells]
                if any(cells):
                    rows.append(cells)

            if not hdr and rows:
                hdr = [f"col_{i+1}" for i in range(max(len(r) for r in rows))]

            width = len(hdr) if hdr else (max(len(r) for r in rows) if rows else 0)
            if width == 0 or not rows:
                continue

            norm_rows = []
            for r in rows:
                if len(r) < width:
                    r = r + [""] * (width - len(r))
                elif len(r) > width:
                    r = r[:width]
                norm_rows.append(r)

            stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
            out_path = CSV_DIR / f"turo_earnings_scraped_{stamp}.csv"
            with open(out_path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if hdr:
                    w.writerow(hdr)
                w.writerows(norm_rows)

            log(f"[fallback] Scraped table -> {out_path} ({hint})")
            return out_path

        except Exception:
            continue
    return None


# ----------------------------
# Download flow
# ----------------------------
def click_download_and_save(page: Page) -> Path:
    # Ensure we are on the business earnings page (the one with the visible Download CSV button)
    year = datetime.utcnow().year
    try:
        page.goto(f"https://turo.com/us/en/business/earnings?year={year}", wait_until="domcontentloaded")
    except Exception:
        pass
    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
    except Exception:
        pass

    # Make sure overlays are gone and the button is in view
    _close_chat_and_overlays(page)
    try:
        page.mouse.wheel(0, 2000)
        page.wait_for_timeout(500)
    except Exception:
        pass

    # Robust locator candidates for "Download CSV"
    locators = [
        page.get_by_role("button", name=re.compile(r"Download CSV", re.I)).first,
        page.get_by_text(re.compile(r"^\\s*Download\\s+CSV\\s*$", re.I)).first,
        page.locator("button:has-text('Download CSV')").first,
        page.locator("a:has-text('Download CSV')").first,
        page.locator("css=[data-testid*='download' i]").filter(has_text=re.compile(r"CSV", re.I)).first,
        page.locator("xpath=//*[self::button or self::a or self::*[@role='button']][contains(normalize-space(.), 'Download CSV')]").first,
    ]

    # Try each locator: scroll into view and click inside expect_download
    for loc in locators:
        try:
            if loc and loc.count() > 0:
                el = loc
                el.scroll_into_view_if_needed(timeout=2000)
                with page.expect_download(timeout=30_000) as dl:
                    el.click(force=True, timeout=5000)
                download = dl.value
                stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
                target = CSV_DIR / f"turo_earnings_{stamp}.csv"
                download.save_as(str(target))
                log(f"Downloaded CSV -> {target}")
                return target
        except Exception:
            # Try a small nudge scroll and continue
            try:
                page.mouse.wheel(0, 1200)
                page.wait_for_timeout(300)
            except Exception:
                pass

    # FINAL JS fallback: query any element whose text includes "Download CSV" and click it
    try:
        found = page.evaluate_handle("""
() => {
  const nodes = Array.from(document.querySelectorAll('button, a, [role="button"], [data-testid], [class]'));
  const needle = /download\\s*csv/i;
  for (const n of nodes) {
    const t = (n.innerText || n.textContent || '').trim();
    if (needle.test(t)) return n;
  }
  return null;
}
""")
        if found and found.as_element():
            page.evaluate("(n) => n.scrollIntoView({behavior:'instant', block:'center'})", found)
            with page.expect_download(timeout=30_000) as dl:
                page.evaluate("(n) => n.click()", found)
            download = dl.value
            stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
            target = CSV_DIR / f"turo_earnings_{stamp}.csv"
            download.save_as(str(target))
            log(f"Downloaded CSV (JS fallback) -> {target}")
            return target
    except Exception:
        pass

    # If still failing, dump debug artifacts as before
    html_path = OUT_DIR / "debug_earnings_page.html"
    png_path = OUT_DIR / "debug_earnings_page.png"
    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        html = page.content()
        html_path.write_text(html, encoding="utf-8")
        page.screenshot(path=str(png_path), full_page=True)
        log(f"[debug] Could not click Download CSV. Saved HTML: {html_path}, screenshot: {png_path}")
        log(f"[debug] Current URL: {page.url}")
    except Exception:
        pass

    raise RuntimeError("Could not click the 'Download CSV' button on business earnings page.")



    # Try frames
    for f in page.frames:
        if f == page.main_frame:
            continue
        try:
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

    # Debug artifacts
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

    # Dump visible controls to logs
    try:
        dump_controls(page, tag="main")
    except Exception:
        pass
    for f in page.frames:
        if f == page.main_frame:
            continue
        try:
            dump_controls(f, tag=f"frame:{f.url[:120]}")
        except Exception:
            pass

    # Fallback: sniffed CSV from network?
    if csv_bytes.get("buf"):
        stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        target = CSV_DIR / f"turo_earnings_sniffed_{stamp}.csv"
        with open(target, "wb") as fo:
            fo.write(csv_bytes["buf"])
        log(f"[fallback] Saved sniffed CSV -> {target}")
        return target

    # Fallback: scrape table
    try:
        scraped = _extract_table_to_csv(page, hint="main")
        if scraped:
            return scraped
        for f in page.frames:
            if f == page.main_frame:
                continue
            if any(x in (f.url or "") for x in ["ads", "doubleclick", "googletag", "tracking"]):
                continue
            scraped = _extract_table_to_csv(f, hint=f.url)
            if scraped:
                return scraped
    except Exception:
        pass

    raise RuntimeError("Could not find the 'Download CSV' control. Inspect debug artifacts and adjust selectors.")


# ----------------------------
# Main
# ----------------------------
def main(headless: bool):
    ensure_dirs()
    storage_state_path = os.environ.get("AUTH_STORAGE_STATE", "").strip()
    is_ci = os.environ.get("GITHUB_ACTIONS") == "true"
    launch_args = ["--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage", "--no-sandbox"]

    with sync_playwright() as p:
        if storage_state_path and Path(storage_state_path).exists():
            log(f"Using storage_state: {storage_state_path}")
            browser = p.chromium.launch(headless=(False if is_ci else headless), args=launch_args)
            context = browser.new_context(
                storage_state=storage_state_path,
                accept_downloads=True,
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 900},
                locale="en-US",
                timezone_id="America/New_York",
            )
            context.add_init_script("""
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
Object.defineProperty(navigator, 'platform', { get: () => 'MacIntel' });
Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
""")
            page = context.new_page()

            # Network sniff for CSV
            def _maybe_capture_csv(resp):
                try:
                    ct = (resp.headers or {}).get("content-type", "")
                    url = (resp.url or "").lower()
                    if ".csv" in url or "text/csv" in ct.lower():
                        csv_bytes["buf"] = resp.body()
                except Exception:
                    pass

            page.on("response", _maybe_capture_csv)

        else:
            log("Using persistent local profile (.pw-user)")
            browser = p.chromium.launch_persistent_context(
                user_data_dir=str(USER_DATA_DIR),
                headless=headless,
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

        # Generous timeout at PAGE level
        page.set_default_timeout(5 * 60 * 1000)

        try:
            go_to_earnings(page)

            # Detect login redirect early
            cur = page.url or ""
            if "login" in cur or "auth" in cur:
                page.screenshot(path=str(OUT_DIR / "login_redirect.png"))
                raise RuntimeError("Session cookie invalid: redirected to login. Recreate storage_state.json and update TURO_STORAGE_STATE_B64.")

            # Try a tab that commonly exposes export
            maybe_click_default_tab(page)

            # Local (non-headless): allow manual login if export text isn't visible fast
            if not storage_state_path and not headless:
                try:
                    page.get_by_text(re.compile(r"export|csv", re.I)).first.wait_for(timeout=3000)
                except Exception:
                    wait_for_manual_login(page)
                    go_to_earnings(page)
                    maybe_click_default_tab(page)

            # Attempt the download (or fallbacks)
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

    # Run ETL after successful download/scrape/sniff
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
