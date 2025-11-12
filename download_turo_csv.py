#!/usr/bin/env python3
"""
download_turo_csv.py

Automates download of Turo Host Earnings/Transactions CSV and runs ETL to update turo.duckdb.

Features:
- CI-friendly using AUTH_STORAGE_STATE cookie (no MFA each run)
- Local persistent profile for manual login/MFA when needed
- Stealth/anti-bot init scripts, headful/headless control
- Robust CSV click (buttons, links, menus, kebab), iframe scan
- Network-sniff fallback if CSV is fetched via XHR
- Table-scrape fallback to keep pipeline green
- Debug artifacts (HTML + screenshot + control dumps) in out/

Environment (CI):
  AUTH_STORAGE_STATE=auth/storage_state.json
  PLAYWRIGHT_HEADLESS=0 (recommended; run under Xvfb in GitHub Actions)

Usage:
  Local:
    python download_turo_csv.py
    python download_turo_csv.py --headless
  CI:
    xvfb-run -a python download_turo_csv.py
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

# Known working host dashboard targets (avoid public 'business' marketing shell)
HOST_TARGETS = [
    "https://turo.com/host/transactions",
    "https://turo.com/host/earnings",
    "https://turo.com/host/trips",
    # locale-prefixed (some accounts bounce)
    "https://turo.com/us/en/host/transactions",
    "https://turo.com/us/en/host/earnings",
    "https://turo.com/us/en/host/trips",
]

# CSV capture via network sniff
csv_bytes = {"buf": None}


# ----------------------------
# Utilities / logging
# ----------------------------
def ensure_dirs() -> None:
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def log(msg: str) -> None:
    print(f"[download] {msg}", flush=True)


# ----------------------------
# Navigation / overlays
# ----------------------------
def wait_for_manual_login(page: Page) -> None:
    page.bring_to_front()
    log("\n==> Please complete Turo login/MFA in the opened browser window.")
    log("    After you SEE the Host dashboard, return here and press ENTER.")
    input("==> Press ENTER to continue... ")


def _close_overlays(scope: Page) -> None:
    # Common cookie/chat banners
    for txt in [
        r"Accept( all)? cookies",
        r"\bAccept\b",
        r"Got it",
        r"I understand",
        r"Dismiss",
        r"Close",
        r"Restart chat",
        r"Ask Turo",
    ]:
        try:
            scope.get_by_role("button", name=re.compile(txt, re.I)).first.click(timeout=1200)
        except Exception:
            pass
    try:
        scope.get_by_text(re.compile(r"\baccept\b|\bagree\b", re.I)).first.click(timeout=1000)
    except Exception:
        pass
    # Remove pesky chat iframes/bubbles if present
    try:
        scope.evaluate(
            """
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
"""
        )
    except Exception:
        pass


def go_to_host_finance(page: Page) -> None:
    """
    Force navigation into the host dashboard finance area where the CSV export lives.
    Tries multiple deep links and then reveals finance tabs if needed.
    """
    for url in HOST_TARGETS:
        try:
            page.goto(url, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=8000)
            except Exception:
                pass

            # Skip if we somehow got sent to public marketing pages
            cur = page.url or ""
            if "/business/earnings" in cur:
                continue

            # Try to reveal finance view via tabs/left-rail
            for label in ["Transactions", "Earnings", "Reports", "Trips", "Payouts"]:
                try:
                    page.get_by_role("tab", name=re.compile(label, re.I)).first.click(timeout=1200)
                    time.sleep(0.3)
                    break
                except Exception:
                    try:
                        page.get_by_role("link", name=re.compile(label, re.I)).first.click(timeout=1200)
                        time.sleep(0.3)
                        break
                    except Exception:
                        pass

            # Heuristic: confirm we’re not in public chrome
            try:
                page.get_by_text(
                    re.compile(r"(Transactions|Earnings|Payout|Export|CSV|Gross|Net|Trip)", re.I)
                ).first.wait_for(timeout=3000)
                return  # success
            except Exception:
                continue
        except Exception:
            continue

    raise RuntimeError("Could not navigate into Host finance pages (Transactions/Earnings).")


# ----------------------------
# CSV control discovery
# ----------------------------
def _candidate_locators(scope: Page):
    """Return locator factories to probe for CSV/Export controls."""
    locs = []
    # Text labels
    for label in [
        r"\bCSV\b",
        r"Download CSV",
        r"Export CSV",
        r"Export .*CSV",
        r"\bExport\b",
        r"\bDownload\b",
        r"Download report",
        r"Export transactions",
    ]:
        locs.append(lambda s=scope, l=label: s.get_by_role("button", name=re.compile(l, re.I)))
        locs.append(lambda s=scope, l=label: s.get_by_role("link",   name=re.compile(l, re.I)))
        locs.append(lambda s=scope, l=label: s.get_by_text(re.compile(l, re.I)))

    # Attribute/CSS-based
    for sel in [
        "[data-testid*=csv i]",
        "[data-testid*=export i]",
        "[data-testid*=download i]",
        "[data-qa*=csv i]",
        "[aria-label*=csv i]",
        "[aria-label*=export i]",
        "button:has-text('CSV')",
        "button:has-text('Export')",
        "a:has-text('CSV')",
        "a:has-text('Export')",
        "button:has-text('Export transactions')",
        "a:has-text('Export transactions')",
    ]:
        locs.append(lambda s=scope, css=sel: s.locator(css))

    # Direct anchors to CSV
    for sel in [
        "a[download$='.csv']",
        "a[href$='.csv']",
        "a[href*='.csv?']",
    ]:
        locs.append(lambda s=scope, css=sel: s.locator(sel))

    return locs


def _find_and_click_csv(scope: Page) -> bool:
    _close_overlays(scope)

    # Plain visible controls
    for make in _candidate_locators(scope):
        try:
            loc = make()
            if loc and loc.count() > 0:
                for i in range(min(loc.count(), 8)):
                    el = loc.nth(i)
                    try:
                        if el.is_visible():
                            el.scroll_into_view_if_needed(timeout=1500)
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
            for txt in [r"\bCSV\b", r"Export CSV", r"Download CSV", r"Export transactions"]:
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
    # Let dashboard settle
    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
    except PWTimeout:
        pass

    _close_overlays(page)
    try:
        page.mouse.wheel(0, 1500)
        page.wait_for_timeout(300)
    except Exception:
        pass

    # 1) Try main page controls
    if _find_and_click_csv(page):
        with page.expect_download(timeout=30_000) as dl:
            pass
        download = dl.value
        stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        target = CSV_DIR / f"turo_earnings_{stamp}.csv"
        download.save_as(str(target))
        log(f"Downloaded CSV -> {target}")
        return target

    # 2) Try frames (some views render in iframes)
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

    # 3) Debug artifacts
    html_path = OUT_DIR / "debug_earnings_page.html"
    png_path = OUT_DIR / "debug_earnings_page.png"
    try:
        html = page.content()
        html_path.write_text(html, encoding="utf-8")
        page.screenshot(path=str(png_path), full_page=True)
        log(f"[debug] Could not find an export control. Saved HTML: {html_path}, screenshot: {png_path}")
        log(f"[debug] Current URL: {page.url}")
    except Exception:
        pass

    # 4) Dump visible controls to logs
    try:
        dump_controls(page, tag="main")
    except Exception:
        pass
    for f in page.frames:
        if f == page.main_frame:
            continue
        try:
            dump_controls(f, tag=f"frame:{(f.url or '')[:120]}")
        except Exception:
            pass

    # 5) Fallback: sniffed CSV from network?
    if csv_bytes.get("buf"):
        stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        target = CSV_DIR / f"turo_earnings_sniffed_{stamp}.csv"
        with open(target, "wb") as fo:
            fo.write(csv_bytes["buf"])
        log(f"[fallback] Saved sniffed CSV -> {target}")
        return target

    # 6) Fallback: scrape table if visible
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

    raise RuntimeError("Could not obtain CSV (no export control; see debug artifacts).")


# ----------------------------
# Main
# ----------------------------
def main(headless: bool):
    ensure_dirs()
    storage_state_path = os.environ.get("AUTH_STORAGE_STATE", "").strip()
    is_ci = os.environ.get("GITHUB_ACTIONS") == "true"
    launch_args = ["--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage", "--no-sandbox"]

    # Decide headless from env if present
    env_headless = os.environ.get("PLAYWRIGHT_HEADLESS")
    if env_headless is not None:
        headless_flag = env_headless.strip().lower() not in ("0", "false", "no")
    else:
        headless_flag = headless

    with sync_playwright() as p:
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

        # Generous timeout at PAGE level
        page.set_default_timeout(5 * 60 * 1000)

        try:
            # Navigate into host dashboard (avoid /business/ pages)
            go_to_host_finance(page)

            # Detect login redirect early
            cur = page.url or ""
            if "login" in cur or "auth" in cur:
                page.screenshot(path=str(OUT_DIR / "login_redirect.png"))
                raise RuntimeError(
                    "Session cookie invalid: redirected to login. Recreate storage_state.json and update TURO_STORAGE_STATE_B64."
                )

            # Local (non-headless) helper: allow manual login if export text isn't visible fast
            if not storage_state_path and not headless_flag:
                try:
                    page.get_by_text(re.compile(r"export|csv", re.I)).first.wait_for(timeout=3000)
                except Exception:
                    wait_for_manual_login(page)
                    go_to_host_finance(page)

            # Attempt the download (or fallbacks)
            _ = click_download_and_save(page)

        finally:
            try:
                # Close contexts cleanly
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
    parser.add_argument("--headless", action="store_true", help="Run in headless mode.")
    args = parser.parse_args()
    main(headless=args.headless)
