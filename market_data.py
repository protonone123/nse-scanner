#!/usr/bin/env python3
"""
market_data.py — NSE Market Microstructure Data Layer (Phase 2)
===============================================================
All data sourced from free NSE public endpoints.

Modules
-------
  Bhavcopy   (DS3) — daily delivery % per stock. High delivery = institutional
                      accumulation. Unavailable via yfinance. NSE archives CSV.
  FII/DII    (DS4) — net institutional cash flow. Single most important macro
                      signal for Indian markets. NSE public API.
  GSM Filter (MM3) — Graded Surveillance Measure list. GSM stocks have trading
                      restrictions; breakout signals on them are unexecutable.
                      NSE public API.

Public API
----------
  get_bhavcopy(trade_date)          → dict {SYM: {"deliv_pct": float, "deliv_qty": int, "tot_qty": int}}
  get_delivery_pct(sym, trade_date) → float | None
  get_fii_dii_today()               → dict {"fii_net_cr": float, "dii_net_cr": float, "date": str, "sentiment": str}
  is_gsm_stock(sym)                 → bool
  get_gsm_stocks()                  → set[str]
  get_market_context()              → dict  (merged: FII/DII + GSM count + market sentiment)

Cache
-----
  All data cached in price_cache.db (shared DB).
  Bhavcopy: per-date key (permanent once downloaded for a date).
  FII/DII:  same-day TTL.
  GSM:      7-day TTL (list refreshed weekly by NSE).
"""

import os
import json
import time
import logging
import sqlite3
import requests
from io import StringIO
from datetime import date, datetime, timedelta
from threading import Lock

import pandas as pd

log = logging.getLogger("market_data")

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
CACHE_PATH = os.path.join(BASE_DIR, "price_cache.db")

# ── DB ────────────────────────────────────────────────────────────────────────
_db_lock = Lock()
_db_con  = None

def _get_db():
    global _db_con
    with _db_lock:
        if _db_con is None:
            _db_con = sqlite3.connect(CACHE_PATH, check_same_thread=False)
            _db_con.execute("PRAGMA journal_mode=WAL")
            _db_con.execute("PRAGMA synchronous=NORMAL")
            _db_con.executescript("""
                CREATE TABLE IF NOT EXISTS bhavcopy_cache (
                    trade_date TEXT NOT NULL,
                    symbol     TEXT NOT NULL,
                    deliv_pct  REAL,
                    deliv_qty  INTEGER,
                    tot_qty    INTEGER,
                    PRIMARY KEY (trade_date, symbol)
                );
                CREATE TABLE IF NOT EXISTS fii_dii_cache (
                    trade_date  TEXT PRIMARY KEY,
                    fii_buy_cr  REAL,
                    fii_sell_cr REAL,
                    fii_net_cr  REAL,
                    dii_buy_cr  REAL,
                    dii_sell_cr REAL,
                    dii_net_cr  REAL,
                    sentiment   TEXT,
                    updated_at  TEXT
                );
                CREATE TABLE IF NOT EXISTS gsm_cache (
                    symbol      TEXT PRIMARY KEY,
                    gsm_stage   TEXT,
                    updated_date TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_bhav_date ON bhavcopy_cache(trade_date);
            """)
            _db_con.commit()
        return _db_con

# ── NSE session ───────────────────────────────────────────────────────────────
_NSE_SESSION = None
_NSE_LOCK    = Lock()
_NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

def _get_nse_session() -> requests.Session:
    global _NSE_SESSION
    with _NSE_LOCK:
        if _NSE_SESSION is None:
            sess = requests.Session()
            sess.headers.update(_NSE_HEADERS)
            try:
                sess.get("https://www.nseindia.com", timeout=15)
                time.sleep(0.5)
                sess.get("https://www.nseindia.com/market-data/live-equity-market", timeout=10)
                time.sleep(0.3)
            except Exception as e:
                log.debug(f"NSE session warmup: {e}")
            _NSE_SESSION = sess
    return _NSE_SESSION

def _reset_nse_session():
    global _NSE_SESSION
    with _NSE_LOCK:
        _NSE_SESSION = None

def _nse_get(endpoint: str, retries: int = 3, params: dict = None) -> dict | list | None:
    url = f"https://www.nseindia.com{endpoint}"
    for attempt in range(retries):
        try:
            sess = _get_nse_session()
            resp = sess.get(url, params=params, timeout=20)
            if resp.status_code in (401, 403):
                _reset_nse_session()
                time.sleep(4 * (attempt + 1))
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.JSONDecodeError:
            log.debug(f"NSE {endpoint}: non-JSON response")
            return None
        except Exception as e:
            log.debug(f"NSE {endpoint} attempt {attempt+1}: {e}")
            if attempt < retries - 1:
                time.sleep(3 * (attempt + 1))
    return None

# ================================================================
# BHAVCOPY — DS3
# NSE end-of-day file with delivery % per stock.
#
# Primary URL (NSE archives, direct CSV download, no auth needed):
#   https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{DDMMYYYY}.csv
#
# Columns we use:
#   SYMBOL, SERIES, DELIV_QTY, DELIV_PER, TOTTRDQTY
#
# DELIV_PER = delivery volume / total traded volume × 100
# High DELIV_PER (>60%) = institutions routing delivery trades (accumulation)
# Low DELIV_PER (<20%)  = speculative/intraday only = suspect breakout quality
# ================================================================

def _bhavcopy_url(trade_date: date) -> list[str]:
    """Return candidate URLs for a given trade date (NSE changed URL format in 2024)."""
    d = trade_date.strftime("%d%m%Y")
    d2 = trade_date.strftime("%Y%m%d")
    return [
        f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{d}.csv",
        f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{d2}_F_0000.csv",
        # Legacy format (pre-2023)
        f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{d}.csv",
    ]

def get_bhavcopy(trade_date: date | None = None) -> dict:
    """
    Fetch NSE Bhavcopy for a trading date.

    Returns dict keyed by symbol (no .NS):
      {"RELIANCE": {"deliv_pct": 68.5, "deliv_qty": 1234567, "tot_qty": 2000000}, ...}

    Cached permanently per trade_date in bhavcopy_cache table.
    Returns {} on failure (market holiday, weekend, archive not yet published).
    """
    if trade_date is None:
        trade_date = date.today()
        # If market isn't closed yet, use yesterday
        from datetime import datetime as _dt
        now_ist = _dt.now()
        if now_ist.hour < 16:
            trade_date = trade_date - timedelta(days=1)
        # Skip weekends
        while trade_date.weekday() >= 5:
            trade_date -= timedelta(days=1)

    date_str = str(trade_date)

    # Check cache first
    try:
        con = _get_db()
        rows = con.execute(
            "SELECT symbol, deliv_pct, deliv_qty, tot_qty FROM bhavcopy_cache WHERE trade_date=?",
            (date_str,)
        ).fetchall()
        if rows:
            return {sym: {"deliv_pct": dp, "deliv_qty": dq, "tot_qty": tq}
                    for sym, dp, dq, tq in rows}
    except Exception:
        pass

    # Download from NSE archives
    result = {}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "text/html,application/xhtml+xml,application/xml,text/csv,*/*",
        "Referer": "https://www.nseindia.com/",
    }

    for url in _bhavcopy_url(trade_date):
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 404:
                continue
            if not resp.ok:
                log.debug(f"Bhavcopy {url}: HTTP {resp.status_code}")
                continue
            text = resp.text
            if len(text) < 500 or "SYMBOL" not in text.upper():
                continue

            df = pd.read_csv(StringIO(text))
            df.columns = [c.strip().upper() for c in df.columns]

            # Filter EQ series only
            if "SERIES" in df.columns:
                df = df[df["SERIES"].str.strip() == "EQ"]

            # Identify column names across formats
            sym_col   = next((c for c in df.columns if "SYMBOL" in c), None)
            deliv_col = next((c for c in df.columns if "DELIV" in c and "PER" in c), None)
            dqty_col  = next((c for c in df.columns if "DELIV" in c and "QTY" in c and "PER" not in c), None)
            tqty_col  = next((c for c in df.columns if c in ("TOTTRDQTY", "TOTAL_TRADED_QUANTITY", "TTL_TRD_QNTY")), None)

            if sym_col is None:
                log.debug(f"Bhavcopy {url}: no SYMBOL column. Cols: {list(df.columns)}")
                continue

            for _, row in df.iterrows():
                try:
                    sym = str(row[sym_col]).strip().upper()
                    if not sym or sym == "NAN":
                        continue
                    dp = float(row[deliv_col]) if deliv_col and pd.notna(row.get(deliv_col)) else None
                    dq = int(row[dqty_col])    if dqty_col  and pd.notna(row.get(dqty_col))  else None
                    tq = int(row[tqty_col])    if tqty_col  and pd.notna(row.get(tqty_col))  else None
                    if dp is not None and 0 <= dp <= 100:
                        result[sym] = {"deliv_pct": round(dp, 2), "deliv_qty": dq, "tot_qty": tq}
                except Exception:
                    continue

            if result:
                log.info(f"Bhavcopy {date_str}: {len(result)} stocks loaded")
                break
        except Exception as e:
            log.debug(f"Bhavcopy {url}: {e}")
            continue

    if not result:
        log.info(f"Bhavcopy {date_str}: not available (holiday/weekend/archive lag)")
        return {}

    # Cache to DB
    try:
        con = _get_db()
        with _db_lock:
            for sym, d in result.items():
                con.execute(
                    "INSERT OR REPLACE INTO bhavcopy_cache "
                    "(trade_date, symbol, deliv_pct, deliv_qty, tot_qty) VALUES (?,?,?,?,?)",
                    (date_str, sym, d["deliv_pct"], d.get("deliv_qty"), d.get("tot_qty"))
                )
            con.commit()
    except Exception as e:
        log.debug(f"Bhavcopy cache write: {e}")

    return result


def get_delivery_pct(sym: str, trade_date: date | None = None) -> float | None:
    """
    Get delivery % for a single stock on a given date.
    Thin wrapper — fetches full bhavcopy (cached after first call).
    """
    sym_clean = sym.upper().replace(".NS", "").replace(".BO", "").strip()
    bhav = get_bhavcopy(trade_date)
    entry = bhav.get(sym_clean)
    return entry["deliv_pct"] if entry else None


# ================================================================
# FII / DII DAILY FLOW — DS4
# NSE public API — no auth required (session cookie needed).
# Returns net FII/DII cash segment activity for today.
#
# Interpretation:
#   fii_net_cr > 0   = FII buying = bullish macro context
#   fii_net_cr < -500Cr = heavy FII selling = avoid new longs
#   dii_net_cr > 0 while fii negative = DII counter-buying = support
# ================================================================

def get_fii_dii_today() -> dict:
    """
    Fetch today's FII and DII net cash flow from NSE.

    Returns:
      {
        "fii_buy_cr":  float,
        "fii_sell_cr": float,
        "fii_net_cr":  float,    # positive = FII buying
        "dii_buy_cr":  float,
        "dii_sell_cr": float,
        "dii_net_cr":  float,    # positive = DII buying
        "date":        str,
        "sentiment":   str,      # "BULLISH" | "BEARISH" | "MIXED" | "NEUTRAL"
        "source":      str,
      }
    """
    today_str = str(date.today())

    # Same-day cache check
    try:
        con = _get_db()
        row = con.execute(
            "SELECT fii_buy_cr, fii_sell_cr, fii_net_cr, dii_buy_cr, dii_sell_cr, dii_net_cr, sentiment "
            "FROM fii_dii_cache WHERE trade_date=?",
            (today_str,)
        ).fetchone()
        if row:
            fb, fs, fn, db_, ds, dn, sent = row
            return {
                "fii_buy_cr": fb, "fii_sell_cr": fs, "fii_net_cr": fn,
                "dii_buy_cr": db_, "dii_sell_cr": ds, "dii_net_cr": dn,
                "date": today_str, "sentiment": sent, "source": "cache"
            }
    except Exception:
        pass

    result = _fetch_fii_dii_nse()
    if result:
        _cache_fii_dii(today_str, result)
        return result

    # Fallback: try previous trading day if today's data not yet published
    result = _fetch_fii_dii_nse(days_back=1)
    if result:
        result["note"] = "previous_day"
        return result

    return {"fii_net_cr": None, "dii_net_cr": None, "date": today_str,
            "sentiment": "UNKNOWN", "source": "unavailable"}


def _fetch_fii_dii_nse(days_back: int = 0) -> dict | None:
    """Fetch FII/DII from NSE fiidiiTradeReact API."""
    target = date.today() - timedelta(days=days_back)
    # Skip weekends
    while target.weekday() >= 5:
        target -= timedelta(days=1)
    d_str = target.strftime("%d-%m-%Y")

    data = _nse_get(f"/api/fiidiiTradeReact?date={d_str}")
    if not data:
        # Try without date param (returns most recent available)
        data = _nse_get("/api/fiidiiTradeReact")
    if not data:
        return None

    try:
        # NSE returns list of items; look for FII and DII rows
        fii = next((x for x in data if str(x.get("category", "")).upper() == "FII/FPI"), None)
        dii = next((x for x in data if str(x.get("category", "")).upper() == "DII"), None)

        def _cr(val) -> float:
            """Parse ₹ crore value — NSE returns strings like '1,23,456.78'."""
            try:
                return round(float(str(val).replace(",", "")), 2)
            except Exception:
                return 0.0

        fii_buy  = _cr(fii.get("buyValue", 0)) if fii else 0.0
        fii_sell = _cr(fii.get("sellValue", 0)) if fii else 0.0
        fii_net  = round(fii_buy - fii_sell, 2)
        dii_buy  = _cr(dii.get("buyValue", 0)) if dii else 0.0
        dii_sell = _cr(dii.get("sellValue", 0)) if dii else 0.0
        dii_net  = round(dii_buy - dii_sell, 2)

        # Sentiment: FII drives direction; DII modifies conviction
        if fii_net > 500:
            sentiment = "BULLISH"
        elif fii_net > 0:
            sentiment = "MILDLY_BULLISH"
        elif fii_net < -1000:
            sentiment = "BEARISH"    # heavy selling
        elif fii_net < 0 and dii_net > 0:
            sentiment = "MIXED"      # FII selling but DII supporting
        elif fii_net < 0:
            sentiment = "BEARISH"
        else:
            sentiment = "NEUTRAL"

        return {
            "fii_buy_cr": fii_buy, "fii_sell_cr": fii_sell, "fii_net_cr": fii_net,
            "dii_buy_cr": dii_buy, "dii_sell_cr": dii_sell, "dii_net_cr": dii_net,
            "date": str(target), "sentiment": sentiment, "source": "nse_api"
        }
    except Exception as e:
        log.debug(f"FII/DII parse error: {e}")
        return None


def _cache_fii_dii(date_str: str, data: dict):
    try:
        con = _get_db()
        with _db_lock:
            con.execute(
                "INSERT OR REPLACE INTO fii_dii_cache "
                "(trade_date, fii_buy_cr, fii_sell_cr, fii_net_cr, "
                " dii_buy_cr, dii_sell_cr, dii_net_cr, sentiment, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,datetime('now'))",
                (date_str,
                 data.get("fii_buy_cr"), data.get("fii_sell_cr"), data.get("fii_net_cr"),
                 data.get("dii_buy_cr"), data.get("dii_sell_cr"), data.get("dii_net_cr"),
                 data.get("sentiment"))
            )
            con.commit()
    except Exception as e:
        log.debug(f"FII/DII cache write: {e}")


# ================================================================
# GSM FILTER — MM3
# Graded Surveillance Measure: NSE places problematic stocks in
# GSM stages I-IV. Restrictions include upfront margin, trade-to-trade
# settlement, and weekly settlement. Breakout signals on these stocks
# are either unexecutable or extremely high-risk.
#
# NSE endpoint: /api/reportGSM
# Also: /api/reportASM (Additional Surveillance Measure — less severe)
# ================================================================

def get_gsm_stocks(include_asm: bool = False) -> set:
    """
    Return set of symbols currently under GSM (and optionally ASM) surveillance.
    7-day cache. Returns empty set on failure (fail-open, don't block signals).

    include_asm: also include ASM stocks (less severe restrictions, optional filter)
    """
    today_str = str(date.today())

    # 7-day TTL check
    try:
        con = _get_db()
        rows = con.execute(
            "SELECT symbol FROM gsm_cache WHERE updated_date >= ?",
            (str(date.today() - timedelta(days=7)),)
        ).fetchall()
        if rows:
            return {r[0] for r in rows}
    except Exception:
        pass

    gsm_stocks = set()

    # Fetch GSM list
    data = _nse_get("/api/reportGSM")
    if data:
        try:
            items = data if isinstance(data, list) else data.get("data", [])
            for item in items:
                sym = str(item.get("symbol", item.get("SYMBOL", ""))).strip().upper()
                stage = str(item.get("gsmStage", item.get("stage", "GSM"))).strip()
                if sym:
                    gsm_stocks.add(sym)
                    try:
                        con = _get_db()
                        with _db_lock:
                            con.execute(
                                "INSERT OR REPLACE INTO gsm_cache (symbol, gsm_stage, updated_date) VALUES (?,?,?)",
                                (sym, stage, today_str)
                            )
                    except Exception:
                        pass
        except Exception as e:
            log.debug(f"GSM parse error: {e}")

    if include_asm:
        asm_data = _nse_get("/api/reportASM")
        if asm_data:
            try:
                items = asm_data if isinstance(asm_data, list) else asm_data.get("data", [])
                for item in items:
                    sym = str(item.get("symbol", item.get("SYMBOL", ""))).strip().upper()
                    if sym:
                        gsm_stocks.add(sym)
            except Exception:
                pass

    try:
        _get_db().commit()
    except Exception:
        pass

    if gsm_stocks:
        log.info(f"GSM stocks loaded: {len(gsm_stocks)}")
    else:
        log.info("GSM list: 0 stocks (API may be unavailable — fail-open)")

    return gsm_stocks


def is_gsm_stock(sym: str, gsm_set: set | None = None) -> bool:
    """
    Check if a stock is under GSM surveillance.

    Pass pre-fetched gsm_set for performance (one fetch per scan, not per stock).
    If gsm_set is None, fetches from cache or NSE API.
    """
    sym_clean = sym.upper().replace(".NS", "").replace(".BO", "").strip()
    if gsm_set is not None:
        return sym_clean in gsm_set
    return sym_clean in get_gsm_stocks()


# ================================================================
# SECTOR TREND — SC5
# Maps yfinance sector strings to NSE sector index symbols
# that are already tracked in sector_cache by data_updater.
# get_sector_trend() exists in data_updater.py — we provide
# the sector→index mapping here so scanner.py can call it.
# ================================================================

# Maps yfinance sector name → sector_cache key (SECTOR_INDICES keys in data_updater.py)
SECTOR_TO_INDEX = {
    "Technology":            "NIFTYIT",
    "Financial Services":    "NIFTYBANK",
    "Healthcare":            "NIFTYPHARMA",
    "Consumer Defensive":    "NIFTY50",      # proxy
    "Consumer Cyclical":     "NIFTY50",      # proxy
    "Energy":                "RELIANCE",
    "Basic Materials":       "JSWSTEEL",
    "Industrials":           "NIFTY50",
    "Utilities":             "NIFTY50",
    "Communication Services":"TCS",
    "Real Estate":           "NIFTYBANK",    # proxy
}

def get_sector_trend_for_stock(yf_sector: str) -> str:
    """
    Return sector trend for a stock given its yfinance sector string.
    Calls data_updater.get_sector_trend() with the mapped index symbol.
    Returns "Unknown" if sector not mapped or cache empty.
    """
    if not yf_sector:
        return "Unknown"
    index_sym = SECTOR_TO_INDEX.get(yf_sector)
    if not index_sym:
        return "Unknown"
    try:
        from data_updater import get_sector_trend
        return get_sector_trend(index_sym, tf="1d")
    except Exception:
        return "Unknown"


# ================================================================
# MERGED MARKET CONTEXT — called once per daily scan
# ================================================================

def get_market_context() -> dict:
    """
    Return merged market context for the current trading day.
    Call once at scan start; pass to scan_stock() to avoid redundant fetches.

    Returns:
      {
        "fii_net_cr":    float | None,
        "dii_net_cr":    float | None,
        "fii_dii_sentiment": str,        # BULLISH | BEARISH | MIXED | NEUTRAL
        "bhavcopy":      dict,           # {SYM: {deliv_pct, deliv_qty, tot_qty}}
        "gsm_stocks":    set,            # set of GSM-flagged symbols
        "context_ok":    bool,
      }
    """
    log.info("Fetching market context (FII/DII, Bhavcopy, GSM)...")

    fii_dii = get_fii_dii_today()
    bhav    = get_bhavcopy()
    gsm     = get_gsm_stocks()

    log.info(
        f"Market context: FII net ₹{fii_dii.get('fii_net_cr','?')}Cr | "
        f"DII net ₹{fii_dii.get('dii_net_cr','?')}Cr | "
        f"Bhavcopy: {len(bhav)} stocks | GSM: {len(gsm)} stocks"
    )

    return {
        "fii_net_cr":         fii_dii.get("fii_net_cr"),
        "dii_net_cr":         fii_dii.get("dii_net_cr"),
        "fii_buy_cr":         fii_dii.get("fii_buy_cr"),
        "fii_sell_cr":        fii_dii.get("fii_sell_cr"),
        "dii_buy_cr":         fii_dii.get("dii_buy_cr"),
        "dii_sell_cr":        fii_dii.get("dii_sell_cr"),
        "fii_dii_sentiment":  fii_dii.get("sentiment", "UNKNOWN"),
        "bhavcopy":           bhav,
        "gsm_stocks":         gsm,
        "context_ok":         bool(fii_dii.get("fii_net_cr") is not None or bhav),
    }


# ================================================================
# CLI — manual testing
# ================================================================
if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-5s %(message)s")

    ap = argparse.ArgumentParser(description="NSE market data tool")
    ap.add_argument("--bhavcopy", metavar="YYYY-MM-DD", nargs="?", const="today",
                    help="Fetch Bhavcopy for date (default: today/yesterday)")
    ap.add_argument("--sym",     metavar="SYM",  help="Show delivery % for one stock")
    ap.add_argument("--fii",     action="store_true", help="Fetch FII/DII flow today")
    ap.add_argument("--gsm",     action="store_true", help="Fetch GSM stock list")
    ap.add_argument("--context", action="store_true", help="Full market context")
    args = ap.parse_args()

    if args.bhavcopy:
        dt = None if args.bhavcopy == "today" else date.fromisoformat(args.bhavcopy)
        bhav = get_bhavcopy(dt)
        print(f"\nBhavcopy: {len(bhav)} stocks")
        if args.sym:
            sym = args.sym.upper().replace(".NS", "")
            entry = bhav.get(sym)
            if entry:
                print(f"  {sym}: delivery {entry['deliv_pct']}% | "
                      f"deliv qty {entry.get('deliv_qty','?'):,} / total {entry.get('tot_qty','?'):,}")
            else:
                print(f"  {sym}: not in bhavcopy")
        else:
            # Top 20 by delivery %
            top = sorted(bhav.items(), key=lambda x: x[1]["deliv_pct"], reverse=True)[:20]
            for sym, d in top:
                print(f"  {sym:<15} deliv {d['deliv_pct']:5.1f}%")

    if args.fii:
        data = get_fii_dii_today()
        print(f"\nFII/DII — {data.get('date')}")
        print(f"  FII: buy ₹{data.get('fii_buy_cr','?')}Cr  sell ₹{data.get('fii_sell_cr','?')}Cr  NET ₹{data.get('fii_net_cr','?')}Cr")
        print(f"  DII: buy ₹{data.get('dii_buy_cr','?')}Cr  sell ₹{data.get('dii_sell_cr','?')}Cr  NET ₹{data.get('dii_net_cr','?')}Cr")
        print(f"  Sentiment: {data.get('sentiment')}")

    if args.gsm:
        stocks = get_gsm_stocks()
        print(f"\nGSM stocks ({len(stocks)}): {sorted(stocks)[:30]}")

    if args.context:
        ctx = get_market_context()
        print(f"\nMarket context:")
        print(f"  FII net: ₹{ctx['fii_net_cr']}Cr | DII net: ₹{ctx['dii_net_cr']}Cr")
        print(f"  Sentiment: {ctx['fii_dii_sentiment']}")
        print(f"  Bhavcopy: {len(ctx['bhavcopy'])} stocks")
        print(f"  GSM: {len(ctx['gsm_stocks'])} stocks")
