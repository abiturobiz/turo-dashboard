#!/usr/bin/env python3
"""
download_turo_csv.py  —  Abiodun's Turo CSV downloader + ETL

What this script does
---------------------
• Opens Turo host finance pages and tries to export CSV.
• Works locally (persistent profile) or in CI with a provided storage_state.json.
• Adds stealth signals and plenty of debug artifacts if controls aren't found.
• If export is hidden but a table is rendered, scrapes it as a fallback.
• Runs your ETL afterwards to refresh turo.duckdb.

Key environment variables (CI friendly)
---------------------------------------
AUTH_STORAGE_STATE=auth/storage_state.json   # path restored from base64 secret
PLAYWRIGHT_HEADLESS=0|1                      # override --headless flag from env (0=headful)
TURO_LOCALE_PATH=/us/en                      # optional; default '/us/en'
TURO_START_URL=                              # optional; overrides the first URL we hit

Hard truth
----------
If you land on marketing shell (tons of "Car rental" links, "Become a host", etc.)
instead of the authenticated host dashboard, Turo didn't accept your cookie. That
usually means your cookie is device-bound. Use a self-hosted runner or run locally
on a Mac mini and push artifacts to Streamlit.
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

# Locale base for deep links (adjustable via env)
LOCALE_BASE = os.environ.get("TURO_LOCALE_PATH", "/us/en").strip() or "/us/en"

# Primary deep links into host dashboard (authenticated SPA)
HOST_TARGETS = [
    f"https://turo.com/host/transactions",
    f"https://turo.com/host/earnings",
    f"https://turo.com/host/trips",
    f"https://turo.com{LOCALE_BASE}/host/transactions",
    f"https://turo.com{LOCALE_BASE}/host/earnings",
    f"https://turo.com{LOCALE_BASE}/host/trips",
]

# Public “Business Earnings” (marketing shell) where some folks see a CSV only if logged in
BUSINESS_EARNINGS_URL = os.environ.get("TURO_START_URL", f"https://turo.com{LOCALE_BASE}/business/earnings?year={datetime.utcnow().year}")

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


def save_debug(page: Page, stem: str = "debug_earnings_page") -> None:
    try:
        html_path = OUT_DIR / f"{stem}.html"
        png_path = OUT_DIR / f"{stem}.png"
        html = page.content()
        html_path.write_text(html, encoding="utf-8")
        page.screenshot(path=str(png_path), full_page=True)
        log(f"[debug] Saved HTML: {html_path}, screenshot: {png_path}")
        log(f"[debug] Current URL: {page.url}")
    except Exception:
        pass


# ----------------------------
# Host vs Marketing heuristics
# ----------------------------
def looks_like_marketing_shell(page: Page) -> bool:
    """
    Heuristic: marketing shell shows a lot of public links (“Car rental”, “Become a host”, etc.)
    and lacks app tabs (“Transactions”, “Payouts”, “Earnings”) as ARIA roles.
    """
    try:
        # If we can see typical host tabs/links, it's likely NOT marketing.
        host_terms = [r"Transactions", r"Earnings", r"Payouts", r"Trips", r"Export", r"CSV"]
        for term in host_terms:
            try:
                page.get_by_role("tab", name=re.compile(term, re.I)).first.wait_for(timeout=800)
                return False
            except Exception:
                pass
            try:
                page.get_by_role("link", name=re.compile(term, re.I)).first.wait_for(timeout=800)
                return False
            except Exception:
                pass

        # Scan visible links for marketing keywords
        link_texts = _visible_texts(page.locator("a"))
        marketing_hits = sum(
            1
            for t in link_texts
            if re.search(
                r"(Car rental|Classic car rental|Convertible|SUV rental|Become a host|See more makes|browse cars)",
                t, re.I
            )
        )
        return marketing_hits >= 3
    except Exception:
        # If in doubt, don't block – return False (not marketing)
        return False


# ----------------------------
# Navigation helpers
# ----------------------------
def wait_for_manual_login(page: Page) -> None:
    page.bring_to_front()
    log("\n==> Please complete Turo login/MFA in the opened browser window.")
    log("    After you SEE the Host dashboard (Transactions/Earnings), press ENTER here.")
    input("==> Press ENTER to continue... ")


def go_to_host_finance(page: Page) -> None:
    """
    Force navigation into the host dashboard finance area where CSV export lives.
    """
    targets = list(HOST_TARGETS) + [BUSINESS_EARNINGS_URL]
    for url in targets:
        try:
            page.goto(url, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=8000)
            except Exception:
                pass

            # If redirected to explicit login/auth page, bail
            cur = page.url or ""
            if "login" in cur or "auth" in cur:
                raise RuntimeError("login-redirect")

            # If the marketing shell is detected, try next target
            if looks_like_marketing_shell(page):
                continue

            # Try clicking visible tabs/links to reveal finance views
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

            # Confirm something financey is visible
            try:
                page.get_by_text(re.compile(r"(Transactions|Earnings|Payout|Export|CSV|Trip ID)", re.I)).first.wait_for(timeout=3000)
                return
            except Exception:
                # Could still be a partial shell; try next
                continue

        except RuntimeError as e:
            if "login-redirect" in str(e):
                raise
        except Exception:
            continue

    raise RuntimeError("Could not navigate into Host finance pages (Transactions/Earnings).")


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
        r"Restart chat",
        r"Close chat",
    ]:
        try:
            scope.get_by_role("button", name=re.compile(txt, re.I)).first.click(timeout=1200)
        except Exception:
            pass
    # Best effort to remove chat overlays
    try:
        scope.evaluate("""
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
        r"Export transactions",
    ]:
        locs.append(lambda s=scope, l=label: s.get_by_role("button", name=re.compile(l, re.I)))
        locs.append(lambda s=scope, l=label: s.get_by_role("link",   name=re.compile(l, re.I)))
        locs.append(lambda s=scope, l=label: s.get_by_text(re.compile(l, re.I)))

    # Attributes / CSS
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
    """
    Try to click an Export/Download CSV control in the *host dashboard*.
    If none, try the business earnings page. If still none, sniff network or scrape table.
    """
    # 1) Attempt on current page (assumed host dashboard)
    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
    except PWTimeout:
        pass
    _close_banners(page)

    # Primary attempt (host app)
    if _find_and_click_csv(page):
        with page.expect_download(timeout=30_000) as dl:
            pass
        download = dl.value
        stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        target = CSV_DIR / f"turo_earnings_{stamp}.csv"
        download.save_as(str(target))
        log(f"Downloaded CSV -> {target}")
        return target

    # 2) Try any iframes (host app sometimes nests content)
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

    # 3) Try the public “business earnings” page as a last resort
    try:
        page.goto(BUSINESS_EARNINGS_URL, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=8000)
        except Exception:
            pass
        _close_banners(page)
        # Try to find a "Download CSV" on this page
        locs = [
            page.get_by_role("button", name=re.compile(r"Download CSV", re.I)).first,
            page.get_by_text(re.compile(r"\bDownload\s+CSV\b", re.I)).first,
            page.locator("button:has-text('Download CSV')").first,
            page.locator("a:has-text('Download CSV')").first,
            page.locator("[data-testid*=download i]").filter(has_text=re.compile(r"CSV", re.I)).first,
        ]
        for loc in locs:
            try:
                if loc and loc.is_visible():
                    with page.expect_download(timeout=30_000) as dl:
                        loc.click(timeout=3000)
                    download = dl.value
                    stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
                    target = CSV_DIR / f"turo_earnings_{stamp}.csv"
                    download.save_as(str(target))
                    log(f"Downloaded CSV (business earnings) -> {target}")
                    return target
            except Exception:
                pass
    except Exception:
        pass

    # 4) Fallbacks: save debug + sniff network + scrape table
    save_debug(page, "debug_earnings_page")
    dump_controls(page, tag="main")
    for f in page.frames:
        if f == page.main_frame:
            continue
        dump_controls(f, tag=f"frame:{f.url[:120]}")

    if csv_bytes.get("buf"):
        stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        target = CSV_DIR / f"turo_earnings_sniffed_{stamp}.csv"
        with open(target, "wb") as fo:
            fo.write(csv_bytes["buf"])
        log(f"[fallback] Saved sniffed CSV -> {target}")
        return target

    scraped = _extract_table_to_csv(page, hint="main or business")
    if scraped:
        return scraped

    raise RuntimeError("Could not obtain CSV (no export control; see debug artifacts).")


# ----------------------------
# Main
# ----------------------------
def main(headless: bool):
    ensure_dirs()
    storage_state_path = os.environ.get("AUTH_STORAGE_STATE", "").strip()
    is_ci = os.environ.get("GITHUB_ACTIONS") == "true"
    launch_args = ["--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage", "--no-sandbox"]

    # Allow env to override headless (e.g., PLAYWRIGHT_HEADLESS=0)
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
            # Navigate into the host dashboard (or fail clearly if cookie invalid)
            try:
                go_to_host_finance(page)
            except RuntimeError as e:
                save_debug(page, "login_or_marketing_redirect")
                # If redirected to login/auth → definitely invalid/expired cookie
                cur = page.url or ""
                if "login" in cur or "auth" in cur or "signin" in cur:
                    raise RuntimeError("Session cookie invalid: redirected to login. Recreate storage_state.json and update TURO_STORAGE_STATE_B64.") from e
                # If we look like public marketing shell, also treat as invalid cookie
                if looks_like_marketing_shell(page):
                    raise RuntimeError("Not in host dashboard (marketing shell detected). Cookie likely device-bound; use self-hosted runner or refresh storage_state from that machine.") from e
                # Otherwise, bubble up the original message
                raise

            # Attempt the download (or fallbacks)
            _ = click_download_and_save(page)

        finally:
            try:
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
