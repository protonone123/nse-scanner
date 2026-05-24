#!/usr/bin/env python3
"""
fundamentals.py — NSE/BSE Supplemental Fundamental Data Layer
===============================================================
Provides data that yfinance cannot reliably supply for Indian equities.

Cached in price_cache.db (same DB as scanner) with per-function TTLs:
  - Bulk/block deals : same-day cache (fetched once, shared across all scan_stock calls)
  - Piotroski score  : 7-day TTL  (quarterly financials don't change daily)
  - Promoter/pledge  : 7-day TTL  (shareholding pattern filed quarterly)

Public API
----------
  get_bulk_deals_today()                   → dict {SYM: {"value_cr": float, "type": str, "side": str}}
  has_insider_activity(sym, bulk_deals)    → (bool, float|None, str|None)
  get_piotroski_score(sym)                 → int|None  (0-9)
  get_promoter_data(sym)                   → {"pledge_pct": float, "promoter_pct": float}
  get_full_fundamentals(sym)               → merged dict (used by dl_fund enrichment)

Phase-1 gaps addressed
-----------------------
  DS1  fundamentals.py existed in scanner imports but was missing from repo.
         All GAP-F1/F2/F3 score components (pledge, bulk-deal, Piotroski) were dead code.
  DS2  scr_promoter_pct / scr_pledging_pct keys now populated here.
"""

import os
import json
import time
import logging
import sqlite3
from datetime import date, datetime, timedelta
from threading import Lock

import requests
import pandas as pd
import numpy as np

log = logging.getLogger("fundamentals")

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
CACHE_PATH = os.path.join(BASE_DIR, "price_cache.db")

# ── TTLs ──────────────────────────────────────────────────────────────────────
TTL_BULK      = 0   # same-day only — fetched once per scan run
TTL_PIO       = 7   # days — quarterly financials
TTL_PLEDGE    = 7   # days — quarterly shareholding pattern
TTL_NOT_FOUND = 30  # days — symbol confirmed missing on Yahoo; don't retry for a month

# ── Cache DB (shared with scanner/data_updater) ───────────────────────────────
_db_lock = Lock()
_db_con  = None

def _get_db():
    global _db_con
    with _db_lock:
        if _db_con is None:
            _db_con = sqlite3.connect(CACHE_PATH, check_same_thread=False)
            _db_con.execute("PRAGMA journal_mode=WAL")
            _db_con.execute("PRAGMA synchronous=NORMAL")
            # Extended fund_cache: add ttl_days + piotroski/pledge columns
            _db_con.executescript("""
                CREATE TABLE IF NOT EXISTS fund_cache (
                    stock        TEXT PRIMARY KEY,
                    fund_json    TEXT,
                    updated_date TEXT
                );
                CREATE TABLE IF NOT EXISTS fundamentals_ext (
                    stock        TEXT NOT NULL,
                    data_key     TEXT NOT NULL,
                    value_json   TEXT,
                    updated_date TEXT,
                    PRIMARY KEY (stock, data_key)
                );
                CREATE TABLE IF NOT EXISTS bulk_deals_cache (
                    trade_date   TEXT NOT NULL,
                    symbol       TEXT NOT NULL,
                    value_cr     REAL,
                    deal_type    TEXT,
                    side         TEXT,
                    PRIMARY KEY (trade_date, symbol)
                );
            """)
            _db_con.commit()
        return _db_con


def _cache_read_ext(stock: str, data_key: str, ttl_days: int) -> dict | None:
    """Read from fundamentals_ext cache. Returns None if missing or stale."""
    try:
        con = _get_db()
        row = con.execute(
            "SELECT value_json, updated_date FROM fundamentals_ext WHERE stock=? AND data_key=?",
            (stock, data_key)
        ).fetchone()
        if not row or not row[0]:
            return None
        if ttl_days > 0:
            try:
                updated = date.fromisoformat(row[1])
                cached_data = json.loads(row[0])
                # "not_found" symbols use TTL_NOT_FOUND instead of normal TTL —
                # no point retrying weekly, Yahoo Finance will still 404 them.
                effective_ttl = (
                    TTL_NOT_FOUND
                    if isinstance(cached_data, dict) and cached_data.get("reason") == "not_found"
                    else ttl_days
                )
                if (date.today() - updated).days > effective_ttl:
                    return None  # stale
                return cached_data
            except Exception:
                return None
        return json.loads(row[0])
    except Exception:
        return None


def _cache_write_ext(stock: str, data_key: str, data: dict):
    """Write to fundamentals_ext cache."""
    try:
        con = _get_db()
        with _db_lock:
            con.execute(
                "INSERT OR REPLACE INTO fundamentals_ext (stock, data_key, value_json, updated_date) "
                "VALUES (?, ?, ?, ?)",
                (stock, data_key, json.dumps(data), str(date.today()))
            )
            con.commit()
    except Exception as e:
        log.debug(f"Cache write ext {stock}/{data_key}: {e}")


# ================================================================
# NSE SESSION — for API calls requiring cookies
# ================================================================
_NSE_SESSION      = None
_NSE_SESSION_LOCK = Lock()
_NSE_BASE         = "https://www.nseindia.com"

_NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://www.nseindia.com/",
    "X-Requested-With": "XMLHttpRequest",
}


def _get_nse_session() -> requests.Session:
    global _NSE_SESSION
    with _NSE_SESSION_LOCK:
        if _NSE_SESSION is None:
            _NSE_SESSION = _build_nse_session()
        return _NSE_SESSION


def _build_nse_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update(_NSE_HEADERS)
    try:
        # Warm up — sets cookies needed for API calls
        sess.get(_NSE_BASE, timeout=15)
        time.sleep(0.5)
        sess.get(f"{_NSE_BASE}/market-data/live-equity-market", timeout=10)
        time.sleep(0.3)
        log.info("NSE session ready")
    except Exception as e:
        log.warning(f"NSE session warmup partial: {e}")
    return sess


def _reset_nse_session():
    global _NSE_SESSION
    with _NSE_SESSION_LOCK:
        _NSE_SESSION = None


def _nse_get(endpoint: str, retries: int = 3) -> dict | None:
    """GET from NSE API with auto-retry and session refresh."""
    url = f"{_NSE_BASE}{endpoint}"
    for attempt in range(retries):
        try:
            sess = _get_nse_session()
            resp = sess.get(url, timeout=20)
            if resp.status_code == 403 or resp.status_code == 401:
                log.warning(f"NSE {endpoint} got {resp.status_code} — rebuilding session")
                _reset_nse_session()
                time.sleep(3 * (attempt + 1))
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.JSONDecodeError:
            log.debug(f"NSE {endpoint}: not JSON response (attempt {attempt+1})")
        except Exception as e:
            log.debug(f"NSE {endpoint} attempt {attempt+1}: {e}")
            if attempt < retries - 1:
                time.sleep(3 * (attempt + 1))
    return None


# ================================================================
# BULK / BLOCK DEALS  (GAP-F2)
# NSE publishes bulk and block deals for the current trading day.
# We fetch once at scan start and pass the dict to every scan_stock call.
# ================================================================

def get_bulk_deals_today() -> dict:
    """
    Fetch today's bulk and block deals from NSE.

    Returns dict keyed by SYMBOL (uppercase, no .NS suffix):
      {
        "RELIANCE": {"value_cr": 250.5, "type": "BULK", "side": "BUY"},
        "HDFCBANK":  {"value_cr": 120.0, "type": "BLOCK", "side": "SELL"},
      }

    Multiple deals for same stock are summed (net value).
    Cached per trade date — safe to call multiple times in same process.
    """
    today_str = str(date.today())

    # Check same-day cache first
    try:
        con = _get_db()
        rows = con.execute(
            "SELECT symbol, value_cr, deal_type, side FROM bulk_deals_cache WHERE trade_date=?",
            (today_str,)
        ).fetchall()
        if rows:
            result = {}
            for symbol, value_cr, deal_type, side in rows:
                result[symbol] = {"value_cr": value_cr, "type": deal_type, "side": side}
            log.debug(f"Bulk deals: {len(result)} from cache")
            return result
    except Exception:
        pass

    deals: dict = {}

    # ── Bulk deals ────────────────────────────────────────────────────────────
    bulk_data = _nse_get("/api/snapshot-capital-market-largedeal")
    if bulk_data:
        try:
            for item in bulk_data.get("data", []):
                sym = str(item.get("symbol", "")).upper().strip()
                if not sym:
                    continue
                qty   = float(item.get("quantity", 0) or 0)
                price = float(item.get("price", 0) or 0)
                side  = str(item.get("buyOrSell", "")).upper()
                val_cr = round(qty * price / 1e7, 2)
                if sym in deals:
                    deals[sym]["value_cr"] += val_cr
                else:
                    deals[sym] = {"value_cr": val_cr, "type": "BULK", "side": side or "BUY"}
        except Exception as e:
            log.debug(f"Bulk deal parse: {e}")

    # ── Block deals ───────────────────────────────────────────────────────────
    # Block deals have a separate endpoint
    block_data = _nse_get("/api/snapshot-capital-market-blockdeal")
    if block_data:
        try:
            for item in block_data.get("data", []):
                sym = str(item.get("symbol", "")).upper().strip()
                if not sym:
                    continue
                qty   = float(item.get("quantity", 0) or 0)
                price = float(item.get("price", 0) or 0)
                side  = str(item.get("buyOrSell", "")).upper()
                val_cr = round(qty * price / 1e7, 2)
                if sym in deals:
                    deals[sym]["value_cr"] += val_cr
                    deals[sym]["type"] = "BLOCK"  # upgrade to block if both
                else:
                    deals[sym] = {"value_cr": val_cr, "type": "BLOCK", "side": side or "BUY"}
        except Exception as e:
            log.debug(f"Block deal parse: {e}")

    if not deals:
        log.info("Bulk/block deals: 0 today (market may be closed or API unavailable)")
        return {}

    # Cache to DB
    try:
        con = _get_db()
        with _db_lock:
            for sym, d in deals.items():
                con.execute(
                    "INSERT OR REPLACE INTO bulk_deals_cache "
                    "(trade_date, symbol, value_cr, deal_type, side) VALUES (?,?,?,?,?)",
                    (today_str, sym, d["value_cr"], d["type"], d["side"])
                )
            con.commit()
    except Exception:
        pass

    log.info(f"Bulk/block deals fetched: {len(deals)} today")
    return deals


def has_insider_activity(sym: str, bulk_deals: dict) -> tuple[bool, float | None, str | None]:
    """
    Check if stock appears in today's bulk/block deals.

    Parameters
    ----------
    sym          : stock symbol without .NS suffix (e.g. "RELIANCE")
    bulk_deals   : dict returned by get_bulk_deals_today()

    Returns (has_deal, value_cr, deal_type)
    """
    sym_clean = sym.upper().replace(".NS", "").replace(".BO", "").strip()
    if sym_clean in bulk_deals:
        d = bulk_deals[sym_clean]
        return True, d.get("value_cr"), d.get("type", "BULK")
    return False, None, None


# ================================================================
# PIOTROSKI F-SCORE  (GAP-F3)
# 9-point scoring system from Piotroski (2000):
#   Profitability  (4): F1 ROA>0, F2 CFO>0, F3 ΔROA>0, F4 CFO>NI (accruals)
#   Leverage       (3): F5 ΔLeverage<0, F6 ΔCurrent ratio>0, F7 No dilution
#   Efficiency     (2): F8 ΔGross margin>0, F9 ΔAsset turnover>0
#
# ≥7 = strong financials. ≤2 = weak. Used in score10 component 12.
# ================================================================

def get_piotroski_score(sym: str) -> int | None:
    """
    Compute Piotroski F-Score (0-9) with 7-day cache.
    Uses yfinance annual financials — best free option for Indian equities.

    Limitations:
      - yfinance sometimes returns empty financials for Indian companies.
      - Annual only (not trailing 12m) — slight lag for fast-growing names.
      - Returns None (not 0) when data insufficient to score reliably.
    """
    # Cache read
    cached = _cache_read_ext(sym, "piotroski", TTL_PIO)
    if cached is not None:
        return cached.get("score")

    score = None
    try:
        import yfinance as yf
        import logging as _logging
        tk = yf.Ticker(sym)

        # Suppress yfinance's own ERROR-level logs (e.g. "HTTP Error 404: Quote not found").
        # The exception still propagates to our handler — we just don't want
        # yfinance polluting the run log with red ERROR lines for expected 404s.
        _yf_log = _logging.getLogger("yfinance")
        _old_yf_level = _yf_log.level
        _yf_log.setLevel(_logging.CRITICAL)
        try:
            income   = tk.financials      # rows = line items, cols = years
            balance  = tk.balance_sheet
            cashflow = tk.cashflow
        finally:
            _yf_log.setLevel(_old_yf_level)

        if (income is None or income.empty or
            balance is None or balance.empty or
            cashflow is None or cashflow.empty):
            log.debug(f"Piotroski {sym}: empty financials from yfinance")
            _cache_write_ext(sym, "piotroski", {"score": None, "reason": "empty"})
            return None

        def gv(df: pd.DataFrame, *keys, col: int = 0) -> float | None:
            """Get value from financial statement, trying multiple row name variants."""
            for key in keys:
                if key in df.index:
                    try:
                        v = df.loc[key].iloc[col]
                        if v is not None and not (isinstance(v, float) and np.isnan(v)):
                            return float(v)
                    except Exception:
                        continue
            return None

        # ── Current year (col 0), Prior year (col 1) ─────────────────────────
        ni_c  = gv(income,  "Net Income", col=0)
        ni_p  = gv(income,  "Net Income", col=1)
        rev_c = gv(income,  "Total Revenue", "Revenue", col=0)
        rev_p = gv(income,  "Total Revenue", "Revenue", col=1)
        cogs_c = gv(income, "Cost Of Revenue", "Cost of Revenue", col=0) or 0
        cogs_p = gv(income, "Cost Of Revenue", "Cost of Revenue", col=1) or 0

        ta_c  = gv(balance, "Total Assets", col=0)
        ta_p  = gv(balance, "Total Assets", col=1)
        ltd_c = gv(balance, "Long Term Debt", "LongTermDebt", col=0) or 0
        ltd_p = gv(balance, "Long Term Debt", "LongTermDebt", col=1) or 0
        ca_c  = gv(balance, "Current Assets", col=0)
        ca_p  = gv(balance, "Current Assets", col=1)
        cl_c  = gv(balance, "Current Liabilities", col=0)
        cl_p  = gv(balance, "Current Liabilities", col=1)
        shr_c = gv(balance, "Ordinary Shares Number", "Share Issued", col=0)
        shr_p = gv(balance, "Ordinary Shares Number", "Share Issued", col=1)

        cfo_c = gv(cashflow, "Operating Cash Flow", "Total Cash From Operating Activities", col=0)

        # Need at least net income + total assets to proceed
        if ni_c is None or ta_c is None or ta_c <= 0:
            _cache_write_ext(sym, "piotroski", {"score": None, "reason": "insufficient_data"})
            return None

        pts = 0

        # F1: ROA > 0  (positive net income relative to assets)
        roa_c = ni_c / ta_c
        if roa_c > 0:
            pts += 1

        # F2: CFO > 0
        if cfo_c is not None and cfo_c > 0:
            pts += 1

        # F3: ΔROA > 0  (improving profitability)
        if ni_p is not None and ta_p is not None and ta_p > 0:
            roa_p = ni_p / ta_p
            if roa_c > roa_p:
                pts += 1

        # F4: CFO > Net Income  (quality of earnings — accruals low)
        if cfo_c is not None and cfo_c > ni_c:
            pts += 1

        # F5: Leverage decreasing  (long-term debt / total assets)
        if ta_p is not None and ta_p > 0:
            lev_c = ltd_c / ta_c
            lev_p = ltd_p / ta_p
            if lev_c < lev_p:
                pts += 1

        # F6: Current ratio improving
        if (ca_c and ca_p and cl_c and cl_p and cl_c > 0 and cl_p > 0):
            cr_c = ca_c / cl_c
            cr_p = ca_p / cl_p
            if cr_c > cr_p:
                pts += 1

        # F7: No dilution (shares outstanding not materially increased)
        if shr_c is not None and shr_p is not None and shr_p > 0:
            if shr_c <= shr_p * 1.02:   # 2% tolerance for ESOP/bonus
                pts += 1

        # F8: Gross margin improving
        if (rev_c and rev_p and rev_c > 0 and rev_p > 0):
            gm_c = (rev_c - cogs_c) / rev_c
            gm_p = (rev_p - cogs_p) / rev_p
            if gm_c > gm_p:
                pts += 1

        # F9: Asset turnover improving  (revenue efficiency)
        if (rev_c and rev_p and ta_c and ta_p and ta_p > 0):
            at_c = rev_c / ta_c
            at_p = rev_p / ta_p
            if at_c > at_p:
                pts += 1

        score = pts
        log.debug(f"Piotroski {sym}: {score}/9")
    except Exception as e:
        err_str = str(e)
        is_not_found = (
            "Not Found" in err_str or "404" in err_str or
            "Quote not found" in err_str or "no price data" in err_str.lower()
        )
        if is_not_found:
            log.info(f"Piotroski {sym}: symbol not on Yahoo Finance — skipping for {TTL_NOT_FOUND}d")
            _cache_write_ext(sym, "piotroski", {"score": None, "reason": "not_found"})
        else:
            log.debug(f"Piotroski {sym} error: {e}")
            _cache_write_ext(sym, "piotroski", {"score": None, "reason": "exception"})
        return None

    _cache_write_ext(sym, "piotroski", {"score": score})
    return score


# ================================================================
# PROMOTER & PLEDGE DATA  (GAP-F1 / DS2)
# Primary source: screener.in (most reliable free source for Indian
# shareholding pattern data — updated quarterly after BSE filings).
# Fallback: yfinance heldPercentInsiders as proxy for promoter holding.
# ================================================================

def get_promoter_data(sym: str) -> dict:
    """
    Fetch promoter holding % and pledge % with 7-day cache.

    Returns:
      {
        "promoter_pct" : float,   # % of shares held by promoters
        "pledge_pct"   : float,   # % of promoter shares pledged
        "source"       : str,     # "screener" | "yfinance_proxy" | "unavailable"
      }

    Note on pledge_pct semantics:
      The scanner uses pledge_pct as % of TOTAL shares (not % of promoter shares).
      Screener.in reports "pledged % of promoter holding" — we convert to total-shares basis
      by multiplying by (promoter_pct / 100). This matches the interpretation in scanner.py's
      score10 fn (GAP-F1: pledge <5% of total shares outstanding = low risk).
    """
    sym_clean = sym.upper().replace(".NS", "").replace(".BO", "").strip()
    cache_key = "pledge"

    cached = _cache_read_ext(sym_clean, cache_key, TTL_PLEDGE)
    if cached is not None:
        return cached

    result = _fetch_screener_pledge(sym_clean)
    if result["source"] == "unavailable":
        result = _fetch_yf_pledge_proxy(sym)

    _cache_write_ext(sym_clean, cache_key, result)
    return result


def _fetch_screener_pledge(sym: str) -> dict:
    """
    Scrape promoter and pledge data from screener.in.
    Tries consolidated view first, then standalone.
    """
    base = {"promoter_pct": 0.0, "pledge_pct": 0.0, "source": "unavailable"}
    urls_to_try = [
        f"https://www.screener.in/company/{sym}/consolidated/",
        f"https://www.screener.in/company/{sym}/",
    ]
    for url in urls_to_try:
        try:
            resp = requests.get(
                url, timeout=15,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                    "Accept": "text/html,application/xhtml+xml",
                }
            )
            if resp.status_code == 404:
                continue
            if not resp.ok:
                time.sleep(1)
                continue

            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, "html.parser")

            promoter_pct = 0.0
            pledge_of_promoter = 0.0   # % of promoter holding pledged

            # ── Parse shareholding pattern section ────────────────────────────
            # screener.in renders a "Shareholding Pattern" section with a table.
            # The table rows look like: "Promoters" | "65.23%" | ...
            # Pledge info is shown as "% of Promoter Holding pledged" or similar.

            for table in soup.find_all("table"):
                for row in table.find_all("tr"):
                    cells = row.find_all(["td", "th"])
                    if not cells:
                        continue
                    label = cells[0].get_text(strip=True).lower()

                    if "promoter" in label and "group" not in label:
                        # First numeric value in this row = latest quarter promoter %
                        for cell in cells[1:]:
                            txt = cell.get_text(strip=True).replace("%", "").replace(",", "")
                            try:
                                v = float(txt)
                                if 0 < v <= 100:
                                    promoter_pct = v
                                    break
                            except ValueError:
                                continue

                    if "pledge" in label:
                        for cell in cells[1:]:
                            txt = cell.get_text(strip=True).replace("%", "").replace(",", "")
                            try:
                                v = float(txt)
                                if 0 <= v <= 100:
                                    pledge_of_promoter = v
                                    break
                            except ValueError:
                                continue

            # screener.in reports pledge as % of promoter holding.
            # Convert to % of total shares:  pledge_total = pledge_of_promoter * promoter_pct / 100
            pledge_pct_total = round(pledge_of_promoter * promoter_pct / 100, 2)

            if promoter_pct > 0:
                return {
                    "promoter_pct": round(promoter_pct, 2),
                    "pledge_pct": pledge_pct_total,
                    "pledge_of_promoter_pct": round(pledge_of_promoter, 2),
                    "source": "screener",
                }
        except Exception as e:
            log.debug(f"Screener {sym}: {e}")
            time.sleep(0.5)

    return base


def _fetch_yf_pledge_proxy(sym: str) -> dict:
    """
    Fallback: use yfinance heldPercentInsiders as promoter holding proxy.
    Pledge data unavailable via yfinance for Indian equities.
    """
    try:
        import yfinance as yf
        info = yf.Ticker(sym).info or {}
        insider_pct = info.get("heldPercentInsiders")
        if insider_pct is not None:
            return {
                "promoter_pct": round(float(insider_pct) * 100, 2),
                "pledge_pct": 0.0,   # unknown via yfinance
                "source": "yfinance_proxy",
            }
    except Exception:
        pass
    return {"promoter_pct": 0.0, "pledge_pct": 0.0, "source": "unavailable"}


# ================================================================
# MERGED FUNDAMENTALS — called by dl_fund() enrichment
# ================================================================

def get_full_fundamentals(sym: str, bulk_deals: dict | None = None) -> dict:
    """
    Return all supplemental fundamentals for a stock.
    Designed to be called from dl_fund() after the yfinance base fetch.

    Parameters
    ----------
    sym          : ticker with .NS suffix (e.g. "RELIANCE.NS")
    bulk_deals   : pre-fetched dict from get_bulk_deals_today() — pass in to avoid
                   redundant API calls (one global fetch per scan, not per stock).

    Returns dict ready to merge into the fund dict in scanner.py:
      {
        "piotroski_score"  : int|None,
        "scr_pledging_pct" : float,
        "scr_promoter_pct" : float,
        "bulk_deal_cr"     : float|None,   # if deal today
        "bulk_deal_type"   : str|None,
        "_fund_ext_ok"     : bool,
      }
    """
    sym_clean = sym.replace(".NS", "").replace(".BO", "").upper()

    # Piotroski (7-day cached, does yfinance financials call internally)
    pio = get_piotroski_score(sym)

    # Promoter / pledge (7-day cached, does screener.in scrape internally)
    pledge_data = get_promoter_data(sym)

    # Bulk deals (caller pre-fetched and passes in — no extra API call)
    bulk_deal_cr   = None
    bulk_deal_type = None
    if bulk_deals:
        has_deal, val_cr, deal_type = has_insider_activity(sym_clean, bulk_deals)
        if has_deal:
            bulk_deal_cr   = val_cr
            bulk_deal_type = deal_type

    return {
        "piotroski_score"  : pio,
        "scr_pledging_pct" : pledge_data.get("pledge_pct", 0.0),
        "scr_promoter_pct" : pledge_data.get("promoter_pct", 0.0),
        "bulk_deal_cr"     : bulk_deal_cr,
        "bulk_deal_type"   : bulk_deal_type,
        "_fund_ext_ok"     : True,
    }


# ================================================================
# BATCH PRE-FETCH — call from data_updater --daily (GAP-IR5 partial fix)
# Pre-warms Piotroski + pledge cache for all stocks so scan_stock()
# reads from cache without live API calls during the scan window.
# ================================================================

def prefetch_fundamentals(stocks: list[str], workers: int = 2, delay: float = 1.0):
    """
    Pre-fetch Piotroski + pledge data for a list of stocks.
    Call from data_updater --daily BEFORE scanner.py --daily runs.

    workers=2, delay=1.0s: conservative rate-limiting for screener.in.
    Screener.in blocks aggressive scrapers; 1 req/sec is safe.

    Stocks already in cache (within TTL) are skipped automatically.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    today = date.today()
    need = []
    for sym in stocks:
        sym_clean = sym.replace(".NS", "").upper()
        # Check if both piotroski and pledge are cached and fresh
        pio_cached    = _cache_read_ext(sym_clean, "piotroski", TTL_PIO)
        pledge_cached = _cache_read_ext(sym_clean, "pledge", TTL_PLEDGE)
        if pio_cached is None or pledge_cached is None:
            need.append(sym)

    log.info(f"Fundamentals pre-fetch: {len(need)}/{len(stocks)} need update")
    if not need:
        return

    done = 0
    errors = 0
    for sym in need:
        try:
            sym_clean = sym.replace(".NS", "").upper()
            # Piotroski — yfinance, usually faster
            pio = get_piotroski_score(sym)
            # Pledge — screener.in, needs rate limiting
            pledge_data = get_promoter_data(sym_clean)
            done += 1
            if done % 50 == 0:
                log.info(f"  Fundamentals: {done}/{len(need)} done")
        except Exception as e:
            errors += 1
            log.debug(f"Prefetch {sym}: {e}")
        time.sleep(delay)   # conservative rate limit for screener.in

    log.info(f"Fundamentals pre-fetch complete: {done} ok, {errors} errors")


# ================================================================
# CLI — manual invocation for testing
# ================================================================
if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s %(levelname)-5s %(message)s")

    ap = argparse.ArgumentParser(description="Fundamentals data tool")
    ap.add_argument("--bulk",    action="store_true", help="Fetch today's bulk/block deals")
    ap.add_argument("--pio",     metavar="SYM",       help="Piotroski score for symbol")
    ap.add_argument("--pledge",  metavar="SYM",       help="Promoter/pledge data for symbol")
    ap.add_argument("--full",    metavar="SYM",       help="Full fundamentals for symbol")
    ap.add_argument("--prefetch",metavar="FILE",      help="Prefetch for symbols in file (one per line)")
    args = ap.parse_args()

    if args.bulk:
        deals = get_bulk_deals_today()
        print(f"\nBulk/Block deals today: {len(deals)}")
        for sym, d in sorted(deals.items(), key=lambda x: -x[1]["value_cr"])[:20]:
            print(f"  {sym:<15} {d['type']:<6} {d['side']:<5} ₹{d['value_cr']:.1f}Cr")

    if args.pio:
        sym = args.pio.upper()
        if not sym.endswith(".NS"):
            sym += ".NS"
        score = get_piotroski_score(sym)
        print(f"\nPiotroski {sym}: {score}/9")

    if args.pledge:
        sym = args.pledge.upper().replace(".NS", "")
        data = get_promoter_data(sym)
        print(f"\nPromoter data {sym}: {data}")

    if args.full:
        sym = args.full.upper()
        if not sym.endswith(".NS"):
            sym += ".NS"
        deals = get_bulk_deals_today()
        data = get_full_fundamentals(sym, bulk_deals=deals)
        print(f"\nFull fundamentals {sym}:")
        for k, v in data.items():
            print(f"  {k}: {v}")

    if args.prefetch:
        with open(args.prefetch) as f:
            stocks = [line.strip() for line in f if line.strip()]
        prefetch_fundamentals(stocks)


# ================================================================
# SCREENER.IN FUNDAMENTALS — DS5
# Primary source for Indian-specific financial data that yfinance
# either misses, mislabels, or sources with 3-6 month lag.
#
# Fields available free on screener.in (no login required for basic):
#   - Sales growth (TTM, 3yr, 5yr, 10yr CAGR)
#   - Profit growth (same horizons)
#   - ROE, ROCE
#   - Debt/Equity ratio
#   - Current ratio
#   - Dividend yield
#   - Face value
#   - P/E, P/B (often more current than yfinance for NSE)
#   - EPS (TTM)
#   - Market cap (INR Cr)
#   - Quarterly earnings growth (from "Quarters" section)
#
# TTL: 7 days — financials change quarterly, daily refetch wasteful.
# Cache key: "screener_fund" in fundamentals_ext table.
#
# Replaces yfinance `earningsQuarterlyGrowth` and `earningsGrowth`
# for CANSLIM 'C' and 'A' checks (DS5 fix).
# ================================================================

def get_screener_fundamentals(sym: str) -> dict:
    """
    Fetch fundamental data from screener.in for an Indian stock.

    DS5 FIX: yfinance earningsQuarterlyGrowth is often None or wrong for NSE stocks
    because Yahoo maps US fiscal quarters. Screener.in uses Indian fiscal quarters
    and reports the numbers as filed with BSE/NSE directly.

    Parameters
    ----------
    sym : str  — ticker with or without .NS suffix (e.g. "RELIANCE" or "RELIANCE.NS")

    Returns dict with keys:
      sales_growth_ttm    : float | None   (% — TTM vs prior TTM)
      profit_growth_ttm   : float | None   (% — TTM net profit growth)
      profit_growth_3yr   : float | None   (% CAGR)
      profit_growth_5yr   : float | None   (% CAGR)
      roe                 : float | None   (% — Return on Equity)
      roce                : float | None   (% — Return on Capital Employed)
      debt_equity         : float | None   (ratio)
      current_ratio       : float | None
      eps_ttm             : float | None   (₹)
      pe_ratio            : float | None
      pb_ratio            : float | None
      div_yield           : float | None   (%)
      market_cap_cr       : float | None   (₹ Crore)
      quarterly_eps_growth: float | None   (% — latest quarter YoY EPS growth)
      face_value          : float | None
      _source             : str            ("screener" | "unavailable")
    """
    sym_clean = sym.upper().replace(".NS", "").replace(".BO", "").strip()
    cache_key = "screener_fund"
    TTL = 7  # days

    cached = _cache_read_ext(sym_clean, cache_key, TTL)
    if cached is not None:
        return cached

    result = _scrape_screener(sym_clean)
    _cache_write_ext(sym_clean, cache_key, result)
    return result


def _scrape_screener(sym: str) -> dict:
    """
    Scrape screener.in/company/{sym}/ for key financial ratios.
    Tries consolidated view first, then standalone.

    screener.in layout (as of 2024-2025):
      - Top ratios table: Market Cap, Current Price, P/E, Book Value, etc.
      - "Key Ratios" section: ROE, ROCE, Sales growth, Profit growth
      - "Quarterly Results" section: revenue and profit per quarter
    """
    base = {
        "sales_growth_ttm": None, "profit_growth_ttm": None,
        "profit_growth_3yr": None, "profit_growth_5yr": None,
        "roe": None, "roce": None, "debt_equity": None,
        "current_ratio": None, "eps_ttm": None, "pe_ratio": None,
        "pb_ratio": None, "div_yield": None, "market_cap_cr": None,
        "quarterly_eps_growth": None, "face_value": None,
        "_source": "unavailable",
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.screener.in/",
    }

    urls_to_try = [
        f"https://www.screener.in/company/{sym}/consolidated/",
        f"https://www.screener.in/company/{sym}/",
    ]

    for url in urls_to_try:
        try:
            resp = requests.get(url, headers=headers, timeout=20)
            if resp.status_code == 404:
                continue
            if not resp.ok:
                time.sleep(1.5)
                continue
            if "company not found" in resp.text.lower():
                continue

            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, "html.parser")
            result = dict(base)  # fresh copy

            def _pct(txt: str) -> float | None:
                """Parse percent value from text."""
                try:
                    return round(float(txt.strip().replace("%", "").replace(",", "")), 2)
                except Exception:
                    return None

            def _num(txt: str) -> float | None:
                """Parse numeric value from text."""
                try:
                    t = txt.strip().replace(",", "").replace("₹", "").replace("Cr", "")
                    return round(float(t), 4)
                except Exception:
                    return None

            # ── Top ratios ul#top-ratios ──────────────────────────────────────
            # screener.in renders these as <li><span class="name">...</span><span class="value">...</span>
            top_ul = soup.find("ul", id="top-ratios")
            if top_ul:
                for li in top_ul.find_all("li"):
                    name_span = li.find("span", class_="name")
                    val_span  = li.find("span", class_=["value", "number"])
                    if not name_span or not val_span:
                        continue
                    name = name_span.get_text(strip=True).lower()
                    val  = val_span.get_text(strip=True)
                    if "market cap" in name:
                        result["market_cap_cr"] = _num(val)
                    elif "p/e" in name and "industry" not in name:
                        result["pe_ratio"] = _num(val)
                    elif "book value" in name:
                        result["pb_ratio"] = _num(val)   # screener shows book value; PE/BV computed below
                    elif "div yield" in name or "dividend yield" in name:
                        result["div_yield"] = _pct(val)
                    elif "eps" in name:
                        result["eps_ttm"] = _num(val)
                    elif "face value" in name:
                        result["face_value"] = _num(val)
                    elif "debt" in name and "equity" in name:
                        result["debt_equity"] = _num(val)
                    elif "current ratio" in name:
                        result["current_ratio"] = _num(val)
                    elif "roe" in name:
                        result["roe"] = _pct(val)
                    elif "roce" in name:
                        result["roce"] = _pct(val)

            # ── Key Ratios section ────────────────────────────────────────────
            # Look for a section or div with class containing "ratios" or id "ratios"
            for section in soup.find_all(["section", "div"], class_=lambda c: c and "ratio" in c.lower()):
                rows_in = section.find_all("tr") or section.find_all("li")
                for row in rows_in:
                    cells = row.find_all(["td", "th", "span"])
                    if len(cells) < 2: continue
                    label = cells[0].get_text(strip=True).lower()
                    val   = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                    if "sales growth" in label and "3yr" not in label and "5yr" not in label:
                        result["sales_growth_ttm"] = _pct(val)
                    elif "profit growth" in label and "3yr" in label:
                        result["profit_growth_3yr"] = _pct(val)
                    elif "profit growth" in label and "5yr" in label:
                        result["profit_growth_5yr"] = _pct(val)
                    elif "profit growth" in label and "ttm" not in label.replace("3yr","").replace("5yr",""):
                        if result["profit_growth_ttm"] is None:
                            result["profit_growth_ttm"] = _pct(val)
                    elif "roe" in label and result["roe"] is None:
                        result["roe"] = _pct(val)
                    elif "roce" in label and result["roce"] is None:
                        result["roce"] = _pct(val)

            # ── Quarterly Results — compute YoY EPS growth ───────────────────
            # Table with id "quarters" or class "data-table" under quarterly section
            qt = soup.find("table", id="quarters")
            if qt is None:
                qt = soup.find("section", id="quarters")
                if qt:
                    qt = qt.find("table")
            if qt:
                eps_row = None
                for tr in qt.find_all("tr"):
                    label = tr.find("td")
                    if label and "eps" in label.get_text(strip=True).lower():
                        eps_row = tr
                        break
                if eps_row:
                    tds = [td.get_text(strip=True).replace(",", "") for td in eps_row.find_all("td")[1:]]
                    # tds = [most_recent, Q-1, Q-2, Q-3, Q-4_ago, ...]
                    # YoY growth = (tds[0] - tds[4]) / |tds[4]| * 100
                    try:
                        if len(tds) >= 5:
                            curr_eps = float(tds[0])
                            yoy_eps  = float(tds[4])
                            if yoy_eps != 0:
                                result["quarterly_eps_growth"] = round(
                                    (curr_eps - yoy_eps) / abs(yoy_eps) * 100, 2
                                )
                    except Exception:
                        pass

            result["_source"] = "screener"
            log.debug(f"Screener {sym}: OK — ROE={result['roe']}% ROCE={result['roce']}% "
                      f"EPS_growth={result['quarterly_eps_growth']}%")
            return result

        except Exception as e:
            log.debug(f"Screener scrape {sym}: {e}")
            time.sleep(1.0)

    return base


def enrich_fund_with_screener(sym: str, fund: dict) -> dict:
    """
    DS5: Enrich a yfinance fund dict with screener.in data.

    Overwrites fields where screener.in is more reliable for Indian equities:
      earningsQuarterlyGrowth → quarterly_eps_growth (YoY, from quarterly table)
      earningsGrowth          → profit_growth_ttm (Indian fiscal year basis)

    Also adds net-new fields not in yfinance:
      roe, roce, debt_equity, current_ratio, profit_growth_3yr, profit_growth_5yr,
      market_cap_cr (INR Cr vs USD in yfinance), quarterly_eps_growth.

    Parameters
    ----------
    sym  : ticker (with or without .NS)
    fund : existing fund dict from dl_fund()

    Returns enriched copy (does not mutate input).
    """
    scr = get_screener_fundamentals(sym)
    if scr.get("_source") == "unavailable":
        return fund  # no enrichment possible — return as-is

    enriched = dict(fund)

    # Overwrite yfinance quarterly growth with screener.in quarterly EPS growth (DS5)
    if scr.get("quarterly_eps_growth") is not None:
        # Convert from % to decimal to match yfinance's earningsQuarterlyGrowth scale (0.25 = 25%)
        enriched["earningsQuarterlyGrowth"] = scr["quarterly_eps_growth"] / 100.0
        enriched["earningsQuarterlyGrowth_screener"] = scr["quarterly_eps_growth"]  # raw % for display

    # Overwrite annual earnings growth
    if scr.get("profit_growth_ttm") is not None:
        enriched["earningsGrowth"] = scr["profit_growth_ttm"] / 100.0
        enriched["earningsGrowth_screener"] = scr["profit_growth_ttm"]

    # Add net-new fields
    for field in ("roe", "roce", "debt_equity", "current_ratio", "eps_ttm",
                  "pe_ratio", "div_yield", "market_cap_cr", "face_value",
                  "profit_growth_3yr", "profit_growth_5yr", "quarterly_eps_growth",
                  "sales_growth_ttm"):
        if scr.get(field) is not None:
            enriched[field] = scr[field]

    # Use screener.in market cap (INR Cr) to cross-check yfinance USD market cap
    if scr.get("market_cap_cr") and not enriched.get("market_cap_cr"):
        enriched["market_cap_cr"] = scr["market_cap_cr"]

    enriched["_screener_ok"] = True
    return enriched
