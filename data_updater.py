#!/usr/bin/env python3
"""
NSE Data Updater
=================
Separate downloader. Scanner reads cache only — zero downloads during scan.

Modes:
  --bootstrap   Full 10yr 1d/1wk/1mo + 2yr 1h + 60d 15m for all stocks (run once)
  --daily       Append new 1d/1wk/1mo bars since last stored date (run 4:00 PM IST)
  --intraday    Append new 15m + 1h bars for watchlist stocks (run every 30 min)
  --gap         Gap scanner: first 15m bar only, for full universe (run 9:20 AM)
  --sectors     Update sector index data (run with --daily)

Timeframes stored:
  1d  → actual download
  1wk → actual download
  1mo → actual download
  1h  → actual download
  15m → actual download (base for intraday)
  30m → resampled from 15m at query time (no extra storage)
  45m → resampled from 15m at query time
  75m → resampled from 15m at query time

Usage:
  python data_updater.py --bootstrap          # run once, ~45-60 min
  python data_updater.py --daily              # run at 4:00 PM IST daily
  python data_updater.py --intraday           # run every 30 min during market
  python data_updater.py --gap                # run at 9:20 AM IST
  python data_updater.py --sectors            # run with --daily
"""

import os, sys, json, time, sqlite3, argparse, logging, random
from datetime import date, datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from io import StringIO
import warnings
warnings.filterwarnings("ignore")

import yfinance as yf
import pandas as pd
import numpy as np

class RateLimitExhausted(Exception):
    """Raised by dl_batch when 429 persists after all DL_RETRIES. Outer loop handles wait+retry."""
    pass

# ================================================================
# PATHS
# ================================================================
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
CACHE_PATH = os.path.join(BASE_DIR, "price_cache.db")
LOG_DIR    = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# ================================================================
# LOGGING
# ================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(LOG_DIR, f"updater_{date.today()}.log"),
            encoding="utf-8"),
    ],
)
log = logging.getLogger("updater")

# ================================================================
# CONFIG
# ================================================================
_IST = timezone(timedelta(hours=5, minutes=30))
def _now():  return datetime.now(_IST)
def _today(): return _now().date()
def _ist(fmt="%H:%M IST"): return _now().strftime(fmt)

MAX_WORKERS   = 3    # METHOD2: reduced from 5 — less concurrent pressure on Yahoo
DL_RETRIES    = 3
DL_BACKOFF    = 3.0
BATCH_SIZE    = 50   # METHOD4: symbols per yfinance batch call (~43 calls vs 6429)
MARKET_OPEN   = (9, 15)    # IST
MARKET_CLOSE  = (15, 30)   # IST

# NSE Bhavcopy — official EOD data, 1 HTTP call = all 2100+ stocks
BHAVCOPY_URLS = [
    "https://archives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv",
    "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv",
]

# Timeframes to download
TF_EOD      = ["1d", "1wk", "1mo"]
TF_INTRADAY = ["15m", "1h"]

# History lengths
HISTORY = {
    "1d":  "10y",
    "1wk": "10y",
    "1mo": "10y",
    "1h":  "2y",
    "15m": "60d",   # yfinance hard limit for <1h intervals
}

# Sector indices to track
# Verified yfinance-working symbols for NSE sector indices
SECTOR_INDICES = {
    "NIFTY50":      "^NSEI",
    "NIFTYBANK":    "^NSEBANK",
    "INDIAVIX":     "^INDIAVIX",
    # Sector ETFs — these work in yfinance where index symbols don't
    "NIFTYIT":      "0P0001AOT0.BO",   # Nifty IT ETF proxy
    "NIFTYPHARMA":  "0P00017ABI.BO",   # Nifty Pharma ETF proxy
    # Mid-cap proxy via NSE index
    "NIFTYMID50":   "^NSEMDCP50",
    # Individual large-cap proxies for sector trend
    "RELIANCE":     "RELIANCE.NS",     # Energy/consumer bellwether
    "HDFCBANK":     "HDFCBANK.NS",     # Banking bellwether
    "TCS":          "TCS.NS",           # IT bellwether
    "SUNPHARMA":    "SUNPHARMA.NS",     # Pharma bellwether
    "TATAMOTORS":   "TATAMOTORS.NS",    # Auto bellwether
    "JSWSTEEL":     "JSWSTEEL.NS",      # Metal bellwether
}

# ================================================================
# YAHOO SESSION (Chrome impersonation — avoids 401 on CI)
# ================================================================
_YF_SESSION      = None
_YF_SESSION_LOCK = Lock()

def _build_session():
    try:
        from curl_cffi import requests as _cr
        sess = _cr.Session(impersonate="chrome110")
        sess.get("https://finance.yahoo.com", timeout=15)
        log.info("curl_cffi Chrome session ready")
        return sess
    except ImportError:
        log.warning("curl_cffi not installed — may 401. pip install curl_cffi")
        return None
    except Exception as e:
        log.warning(f"Session build failed: {e}")
        return None

def _get_session():
    global _YF_SESSION
    with _YF_SESSION_LOCK:
        if _YF_SESSION is None:
            _YF_SESSION = _build_session()
        return _YF_SESSION

def _reset_session():
    global _YF_SESSION
    with _YF_SESSION_LOCK:
        _YF_SESSION = None

# ================================================================
# GLOBAL RATE-LIMIT CIRCUIT BREAKER
# ================================================================
# When ANY worker thread hits a 429, it calls _set_rate_limit().
# ALL threads then block in _wait_if_rate_limited() before their
# next request. This stops the flood of parallel requests that
# would otherwise keep triggering the rate limit repeatedly.

_RL_LOCK  = Lock()
_RL_UNTIL = 0.0   # epoch seconds; 0 = no limit active

def _set_rate_limit(seconds: int):
    """Set (or extend) the global rate-limit pause window."""
    global _RL_UNTIL
    with _RL_LOCK:
        new_until = time.time() + seconds
        if new_until > _RL_UNTIL:
            _RL_UNTIL = new_until
            log.warning(f"⛔ Rate limit — ALL workers paused for {seconds}s "
                        f"(until {time.strftime('%H:%M:%S', time.localtime(new_until))})")

def _wait_if_rate_limited(caller: str = ""):
    """Block the calling thread until the global rate-limit window expires."""
    remaining = _RL_UNTIL - time.time()
    if remaining > 0:
        log.info(f"  ⏳ [{caller}] rate-limit gate — waiting {remaining:.0f}s")
        time.sleep(remaining + 0.5)   # +0.5s buffer after window clears
    # METHOD2: always add jitter — breaks machine-like cadence Yahoo fingerprints
    time.sleep(random.uniform(0.3, 1.2))

# ================================================================
# DOWNLOAD
# ================================================================
def _normalize_df_index(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame index to tz-naive IST datetime. Fixes ValueError on ISO timestamps."""
    try:
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index, utc=True)
        if df.index.tz is not None:
            df.index = df.index.tz_convert("Asia/Kolkata").tz_localize(None)
    except Exception:
        try:
            df.index = pd.to_datetime(df.index, format="mixed", utc=True).tz_localize(None)
        except Exception:
            pass
    return df

def dl(sym: str, interval: str = "1d", period: str = "1y") -> pd.DataFrame | None:
    for attempt in range(DL_RETRIES):
        # Block here until the global rate-limit window clears.
        # All 5 parallel workers pause here — no flood while banned.
        _wait_if_rate_limited(sym)
        try:
            sess = _get_session()
            kw = {"session": sess} if sess else {}
            import contextlib, io as _io
            with contextlib.redirect_stderr(_io.StringIO()):  # suppress yfinance delisted noise
                df = yf.download(sym, period=period, interval=interval,
                                 auto_adjust=True, progress=False, timeout=25, **kw)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = _normalize_df_index(df)   # CRASH FIX: handles ISO 8601 timestamps
            df = df.dropna()
            return df if len(df) > 0 else None
        except Exception as e:
            msg = str(e)
            if "unconverted data remains" in msg or ("T0" in msg and "+" in msg):
                log.debug(f"ISO timestamp error {sym} attempt {attempt+1} — reset")
                _reset_session(); time.sleep(3 * (attempt + 1))
            elif "401" in msg or "Crumb" in msg or "Unauthorized" in msg:
                log.warning(f"401 {sym} attempt {attempt+1} — reset session")
                _reset_session(); time.sleep(8)
            elif "429" in msg or "rate limit" in msg.lower() or "too many" in msg.lower():
                # Escalating global pause: 60s → 120s → 180s
                # Setting this freezes ALL worker threads at the gate above.
                backoff = 60 * (attempt + 1)
                _set_rate_limit(backoff)
                # This thread also waits (gate will be open when we loop back)
                _wait_if_rate_limited(sym)
            elif "delisted" in msg.lower() or "no price data" in msg.lower():
                log.debug(f"Skip {sym} {interval}: delisted/no data")
                return None  # don't retry delisted stocks
            elif attempt < DL_RETRIES - 1:
                time.sleep(DL_BACKOFF * (attempt + 1))
    return None

def dl_since(sym: str, interval: str, since_date: str) -> pd.DataFrame | None:
    """Download bars only since a given date. Uses yfinance start parameter.
    BUG7 FIX: was missing 429/rate-limit handling — a single 429 during incremental
    update would silently return None, corrupting the cache for that stock.
    Now uses the global circuit breaker so all workers pause together.
    """
    for attempt in range(DL_RETRIES):
        # Block here until the global rate-limit window clears.
        _wait_if_rate_limited(sym)
        try:
            sess = _get_session()
            kw = {"session": sess} if sess else {}
            import contextlib, io as _io
            with contextlib.redirect_stderr(_io.StringIO()):
                df = yf.download(sym, start=since_date, interval=interval,
                                 auto_adjust=True, progress=False, timeout=25, **kw)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = _normalize_df_index(df)   # CRASH FIX: ISO timestamp normalization
            df = df.dropna()
            return df if len(df) > 0 else None
        except Exception as e:
            msg = str(e)
            if "unconverted data remains" in msg or ("T0" in msg and "+" in msg):
                log.debug(f"ISO timestamp error {sym} dl_since attempt {attempt+1}")
                _reset_session(); time.sleep(3 * (attempt + 1))
            elif "401" in msg or "Crumb" in msg or "Unauthorized" in msg:
                log.warning(f"401 {sym} dl_since attempt {attempt+1} — reset session")
                _reset_session(); time.sleep(8)
            elif "429" in msg or "rate limit" in msg.lower() or "too many" in msg.lower():
                # Same escalating global pause as dl(): 60s → 120s → 180s
                backoff = 60 * (attempt + 1)
                _set_rate_limit(backoff)
                _wait_if_rate_limited(sym)
            elif "delisted" in msg.lower() or "no price data" in msg.lower():
                log.debug(f"Skip {sym} {interval}: delisted/no data")
                return None
            elif attempt < DL_RETRIES - 1:
                time.sleep(DL_BACKOFF * (attempt + 1))
    return None

# ================================================================
# BATCH DOWNLOAD — METHOD4: multiple symbols in one HTTP call
# ================================================================
def dl_batch(symbols: list[str], interval: str,
             period: str = None, since_date: str = None) -> dict[str, pd.DataFrame]:
    """
    Download up to BATCH_SIZE symbols in a single yfinance HTTP call.
    Returns {sym: DataFrame}. Cuts ~6429 requests down to ~43 for EOD.

    Args:
        symbols:    list of ticker strings (e.g. ["RELIANCE.NS", "TCS.NS"])
        interval:   yfinance interval ("1d", "1wk", "1mo", "1h", "15m")
        period:     yfinance period string ("10y", "2y" …)  — use OR since_date
        since_date: ISO date string "YYYY-MM-DD"             — use OR period
    """
    for attempt in range(DL_RETRIES):
        _wait_if_rate_limited("batch")
        try:
            sess = _get_session()
            kw   = {"session": sess} if sess else {}
            dl_kw = dict(auto_adjust=True, progress=False,
                         timeout=90, group_by="ticker", **kw)
            if since_date:
                dl_kw["start"] = since_date
            else:
                dl_kw["period"] = period

            import contextlib, io as _io
            with contextlib.redirect_stderr(_io.StringIO()):
                raw = yf.download(symbols, interval=interval, **dl_kw)

            if raw is None or len(raw) == 0:
                return {}

            results: dict[str, pd.DataFrame] = {}

            if isinstance(raw.columns, pd.MultiIndex):
                # Normal multi-ticker result: (OHLCV, ticker) MultiIndex columns
                available = raw.columns.get_level_values(1).unique().tolist()
                for sym in symbols:
                    if sym not in available:
                        continue
                    try:
                        sym_df = raw.xs(sym, axis=1, level=1).copy()
                        sym_df = sym_df.dropna(how="all")
                        sym_df = _normalize_df_index(sym_df)
                        sym_df = sym_df.dropna()
                        if len(sym_df) > 0:
                            results[sym] = sym_df
                    except Exception:
                        pass
            else:
                # yfinance collapses to flat df when only 1 ticker has data
                if len(symbols) == 1:
                    flat = _normalize_df_index(raw.dropna())
                    if len(flat) > 0:
                        results[symbols[0]] = flat

            return results

        except Exception as e:
            msg = str(e)
            if "429" in msg or "rate limit" in msg.lower() or "too many" in msg.lower():
                backoff = 90 * (attempt + 1)   # 90s → 180s → 270s
                log.warning(f"⛔ dl_batch 429 attempt {attempt+1}/{DL_RETRIES} "
                            f"({len(symbols)} syms) — backoff {backoff}s")
                _set_rate_limit(backoff)
                _reset_session()
                _wait_if_rate_limited("batch_retry")
                if attempt == DL_RETRIES - 1:
                    # All retries exhausted on 429 — let outer loop handle longer wait
                    raise RateLimitExhausted(
                        f"{len(symbols)} syms {interval}: 429 after {DL_RETRIES} attempts"
                    )
            elif "401" in msg or "Crumb" in msg or "Unauthorized" in msg:
                log.warning(f"401 batch attempt {attempt+1} — reset session")
                _reset_session()
                time.sleep(8)
            elif attempt < DL_RETRIES - 1:
                time.sleep(DL_BACKOFF * (attempt + 1))
            log.warning(f"dl_batch attempt {attempt+1} {interval} "
                        f"{len(symbols)} syms: {e}")

    return {}


def load_universe() -> list[str]:
    urls = [
        "https://archives.nseindia.com/content/equities/EQUITY_L.csv",
        "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
    ]
    import requests as req
    for url in urls:
        for attempt in range(2):
            try:
                resp = req.get(url, timeout=20,
                               headers={"User-Agent": "Mozilla/5.0"})
                resp.raise_for_status()
                df = pd.read_csv(StringIO(resp.text)).dropna(subset=["SYMBOL"])
                for col in [" SERIES", "SERIES"]:
                    if col in df.columns:
                        df = df[df[col].str.strip() == "EQ"]; break
                syms = [s.strip() + ".NS" for s in df["SYMBOL"].astype(str).tolist()]
                log.info(f"Universe: {len(syms)} stocks")
                return syms
            except Exception as e:
                log.warning(f"Universe {url} attempt {attempt+1}: {e}")
                time.sleep(2)
    # Fallback: read from cache — whatever stocks we already have
    log.warning("Universe URL failed — using cached stock list")
    con = _get_db()
    rows = con.execute(
        "SELECT DISTINCT stock FROM cache_meta WHERE tf='1d'"
    ).fetchall()
    return [r[0] for r in rows] if rows else []

def load_watchlist() -> list[str]:
    """Return .NS symbols from watchlist.json that have a breakout zone."""
    wl_path = os.path.join(BASE_DIR, "watchlist.json")
    if not os.path.exists(wl_path):
        return []
    try:
        with open(wl_path) as f:
            items = json.load(f)
        cutoff = str(_today() - timedelta(days=14))
        valid = [w for w in items
                 if w.get("added_date","") >= cutoff
                 and w.get("breakout_zone", 0) > 0]
        return list({w["stock"] + ".NS" for w in valid})
    except Exception:
        return []

# ================================================================
# CACHE DATABASE
# ================================================================
_db_lock = Lock()
_db_con  = None

def _get_db():
    global _db_con
    with _db_lock:
        if _db_con is None:
            _db_con = sqlite3.connect(CACHE_PATH, check_same_thread=False)
            _db_con.execute("PRAGMA journal_mode=WAL")
            _db_con.execute("PRAGMA synchronous=NORMAL")
            _db_con.execute("PRAGMA cache_size=-65536")   # 64MB page cache
            _db_con.executescript("""
                -- Multi-TF price cache: tf column added
                CREATE TABLE IF NOT EXISTS price_cache (
                    stock  TEXT NOT NULL,
                    tf     TEXT NOT NULL,
                    date   TEXT NOT NULL,
                    open   REAL,
                    high   REAL,
                    low    REAL,
                    close  REAL NOT NULL,
                    volume REAL,
                    PRIMARY KEY (stock, tf, date)
                );
                CREATE TABLE IF NOT EXISTS cache_meta (
                    stock        TEXT NOT NULL,
                    tf           TEXT NOT NULL,
                    last_date    TEXT,
                    last_updated TEXT,
                    bar_count    INTEGER,
                    PRIMARY KEY (stock, tf)
                );
                -- Fundamentals (separate — updated daily)
                CREATE TABLE IF NOT EXISTS fund_cache (
                    stock        TEXT PRIMARY KEY,
                    fund_json    TEXT,
                    updated_date TEXT
                );
                -- RVOL profile: avg volume per time-of-day bucket, per stock
                CREATE TABLE IF NOT EXISTS rvol_profile (
                    stock       TEXT NOT NULL,
                    bucket_min  INTEGER NOT NULL,  -- minutes since midnight IST
                    avg_vol     REAL,
                    sample_days INTEGER,
                    updated     TEXT,
                    PRIMARY KEY (stock, bucket_min)
                );
                -- Sector data
                CREATE TABLE IF NOT EXISTS sector_cache (
                    sector     TEXT NOT NULL,
                    symbol     TEXT NOT NULL,
                    tf         TEXT NOT NULL DEFAULT '1d',
                    date       TEXT NOT NULL,
                    close      REAL NOT NULL,
                    volume     REAL,
                    PRIMARY KEY (symbol, tf, date)
                );
                -- Gap scanner results
                CREATE TABLE IF NOT EXISTS gap_signals (
                    scan_date   TEXT NOT NULL,
                    stock       TEXT NOT NULL,
                    gap_pct     REAL,
                    open_price  REAL,
                    prev_close  REAL,
                    volume      REAL,
                    created_at  TEXT DEFAULT (datetime('now','localtime')),
                    PRIMARY KEY (scan_date, stock)
                );
                -- Indexes
                CREATE INDEX IF NOT EXISTS idx_pc_stock_tf ON price_cache(stock, tf);
                CREATE INDEX IF NOT EXISTS idx_pc_date     ON price_cache(tf, date);
                CREATE INDEX IF NOT EXISTS idx_meta        ON cache_meta(stock, tf);
                CREATE INDEX IF NOT EXISTS idx_rvol        ON rvol_profile(stock);
            """)
            # Schema migration: if old single-TF schema exists, migrate it
            _migrate_old_schema(_db_con)
            _db_con.commit()
        return _db_con

def _migrate_old_schema(con):
    """Migrate old price_cache (no tf column) to new multi-TF schema."""
    try:
        # Check if old schema has no tf column
        cols = [r[1] for r in con.execute("PRAGMA table_info(price_cache)").fetchall()]
        if "tf" not in cols:
            log.info("Migrating old price_cache → new multi-TF schema...")
            con.execute("ALTER TABLE price_cache RENAME TO price_cache_old")
            con.execute("""
                CREATE TABLE price_cache (
                    stock TEXT NOT NULL, tf TEXT NOT NULL, date TEXT NOT NULL,
                    open REAL, high REAL, low REAL, close REAL NOT NULL, volume REAL,
                    PRIMARY KEY (stock, tf, date)
                )""")
            con.execute("""
                INSERT INTO price_cache (stock, tf, date, open, high, low, close, volume)
                SELECT stock, '1d', date, open, high, low, close, volume
                FROM price_cache_old
            """)
            con.execute("DROP TABLE price_cache_old")
            # Migrate cache_meta
            old_meta_cols = [r[1] for r in con.execute("PRAGMA table_info(cache_meta)").fetchall()]
            if "tf" not in old_meta_cols:
                con.execute("ALTER TABLE cache_meta RENAME TO cache_meta_old")
                con.execute("""
                    CREATE TABLE cache_meta (
                        stock TEXT NOT NULL, tf TEXT NOT NULL,
                        last_date TEXT, last_updated TEXT, bar_count INTEGER,
                        PRIMARY KEY (stock, tf)
                    )""")
                con.execute("""
                    INSERT INTO cache_meta (stock, tf, last_updated, bar_count)
                    SELECT stock, '1d', last_updated, bar_count FROM cache_meta_old
                """)
                # Move fund data to fund_cache
                con.execute("""
                    CREATE TABLE IF NOT EXISTS fund_cache (
                        stock TEXT PRIMARY KEY, fund_json TEXT, updated_date TEXT
                    )""")
                try:
                    con.execute("""
                        INSERT OR IGNORE INTO fund_cache (stock, fund_json, updated_date)
                        SELECT stock, fund_json, fund_updated FROM cache_meta_old
                        WHERE fund_json IS NOT NULL
                    """)
                except Exception:
                    pass
                con.execute("DROP TABLE cache_meta_old")
            con.commit()
            log.info("Migration complete")
    except Exception as e:
        log.debug(f"Migration: {e}")

# ================================================================
# CACHE READ / WRITE
# ================================================================
def read_cache(stock: str, tf: str = "1d",
               limit: int = 9999) -> pd.DataFrame | None:
    """Read cached bars for a stock+tf. Returns OHLCV DataFrame."""
    try:
        con = _get_db()
        df = pd.read_sql(
            f"SELECT date,open,high,low,close,volume FROM price_cache "
            f"WHERE stock=? AND tf=? ORDER BY date DESC LIMIT {limit}",
            con, params=(stock, tf)
        )
        if len(df) < 2:
            return None
        df = df.iloc[::-1].reset_index(drop=True)  # oldest first
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")
        df.columns = ["Open", "High", "Low", "Close", "Volume"]
        df.index.name = None
        return df.astype(float)
    except Exception as e:
        log.debug(f"read_cache {stock} {tf}: {e}")
        return None

def write_cache(stock: str, tf: str, df: pd.DataFrame):
    """Upsert OHLCV rows into cache."""
    if df is None or len(df) == 0:
        return 0
    rows = []
    for idx, row in df.iterrows():
        if hasattr(idx, "date"):
            date_str = idx.isoformat()
        else:
            date_str = str(idx)[:19]   # keep time for intraday
        rows.append((
            stock, tf, date_str,
            _f(row, "Open"), _f(row, "High"), _f(row, "Low"),
            _f(row, "Close"), _f(row, "Volume"),
        ))
    if not rows:
        return 0
    con = _get_db()
    last_date = rows[-1][2]
    with _db_lock:
        con.executemany(
            "INSERT OR REPLACE INTO price_cache "
            "(stock,tf,date,open,high,low,close,volume) VALUES (?,?,?,?,?,?,?,?)",
            rows
        )
        con.execute(
            "INSERT OR REPLACE INTO cache_meta (stock,tf,last_date,last_updated,bar_count) "
            "VALUES (?,?,?,?,?)",
            (stock, tf, last_date, str(_today()), len(rows))
        )
        con.commit()
    return len(rows)

def _f(row, col):
    v = row.get(col, row.get(col.lower(), 0))
    return float(v) if v is not None and not (isinstance(v, float) and np.isnan(v)) else 0.0

def get_last_date(stock: str, tf: str) -> str | None:
    """Return the last stored date for a stock+tf."""
    try:
        con = _get_db()
        row = con.execute(
            "SELECT last_date FROM cache_meta WHERE stock=? AND tf=?",
            (stock, tf)
        ).fetchone()
        return row[0] if row else None
    except Exception:
        return None

def read_fund(stock: str) -> dict | None:
    try:
        con = _get_db()
        row = con.execute(
            "SELECT fund_json, updated_date FROM fund_cache WHERE stock=?",
            (stock,)
        ).fetchone()
        if row and row[0] and row[1] == str(_today()):
            return json.loads(row[0])
    except Exception:
        pass
    return None

def write_fund(stock: str, fund: dict):
    try:
        con = _get_db()
        with _db_lock:
            con.execute(
                "INSERT OR REPLACE INTO fund_cache (stock,fund_json,updated_date) VALUES (?,?,?)",
                (stock, json.dumps(fund), str(_today()))
            )
            con.commit()
    except Exception:
        pass

# ================================================================
# RESAMPLE — 30m / 45m / 75m from 15m base
# ================================================================
def resample_tf(df_15m: pd.DataFrame, tf: str) -> pd.DataFrame | None:
    """
    Resample 15m DataFrame to 30m, 45m, or 75m.
    yfinance doesn't provide these — we compute them.
    """
    if df_15m is None or len(df_15m) == 0:
        return None
    rule_map = {"30m": "30min", "45m": "45min", "75m": "75min"}
    rule = rule_map.get(tf)
    if not rule:
        return None
    try:
        df = df_15m.copy()
        df.index = pd.to_datetime(df.index)
        resampled = df.resample(rule, label="left", closed="left").agg({
            "Open":   "first",
            "High":   "max",
            "Low":    "min",
            "Close":  "last",
            "Volume": "sum",
        }).dropna(subset=["Close"])
        return resampled if len(resampled) > 0 else None
    except Exception as e:
        log.debug(f"Resample {tf}: {e}")
        return None

# ================================================================
# LAST COMPLETE BAR — don't include forming candles
# ================================================================
def last_complete_bar(interval_minutes: int) -> datetime:
    """
    Returns the timestamp of the last COMPLETE bar for a given interval.
    E.g. at 10:47 IST with 15m bars: last complete bar started at 10:30.
    """
    now = _now()
    total_min = now.hour * 60 + now.minute
    # Round down to last complete boundary
    last_boundary = (total_min // interval_minutes) * interval_minutes
    # Go one back to ensure the bar has closed
    last_complete = last_boundary - interval_minutes
    # Build datetime
    h, m = last_complete // 60, last_complete % 60
    return now.replace(hour=h, minute=m, second=0, microsecond=0)

# ================================================================
# RVOL PROFILE — time-of-day adjusted volume baseline
# ================================================================
def update_rvol_profile(stock: str, df_15m: pd.DataFrame):
    """
    For each 15min bucket (9:15, 9:30, ... 15:15), compute avg volume
    over all available days. Store in rvol_profile table.
    """
    if df_15m is None or len(df_15m) < 15:
        return
    try:
        df = df_15m.copy()
        df.index = pd.to_datetime(df.index)
        df["bucket_min"] = df.index.hour * 60 + df.index.minute
        df["trade_date"] = df.index.date
        # Group by bucket, compute avg volume across days (min 5 days)
        profile = (df.groupby("bucket_min")["Volume"]
                   .agg(avg_vol="mean", sample_days="count")
                   .reset_index())
        # Need at least 5 sample days for reliability
        profile = profile[profile["sample_days"] >= 5]
        rows = [(stock, int(r.bucket_min), float(r.avg_vol),
                 int(r.sample_days), str(_today()))
                for _, r in profile.iterrows()]
        if not rows:
            return
        con = _get_db()
        with _db_lock:
            con.executemany(
                "INSERT OR REPLACE INTO rvol_profile "
                "(stock,bucket_min,avg_vol,sample_days,updated) VALUES (?,?,?,?,?)",
                rows
            )
            con.commit()
    except Exception as e:
        log.debug(f"RVOL profile {stock}: {e}")

def get_rvol(stock: str, bucket_minutes_since_midnight: int) -> float | None:
    """
    Returns RVOL for a stock at a given time bucket.
    RVOL = current_bar_volume / avg_volume_at_this_time_of_day.
    """
    try:
        con = _get_db()
        row = con.execute(
            "SELECT avg_vol FROM rvol_profile WHERE stock=? AND bucket_min=?",
            (stock, bucket_minutes_since_midnight)
        ).fetchone()
        if not row or not row[0]:
            return None
        # Get actual volume at this bucket today from 15m cache
        df = read_cache(stock, "15m", limit=50)
        if df is None or len(df) == 0:
            return None
        df.index = pd.to_datetime(df.index)
        today_buckets = df[df.index.date == _today()]
        today_bucket = today_buckets[
            today_buckets.index.hour * 60 + today_buckets.index.minute
            == bucket_minutes_since_midnight
        ]
        if len(today_bucket) == 0:
            return None
        actual_vol = float(today_bucket["Volume"].iloc[-1])
        avg_vol = float(row[0])
        return round(actual_vol / avg_vol, 2) if avg_vol > 0 else None
    except Exception:
        return None

# ================================================================
# VWAP — from today's 15m bars
# ================================================================
def calc_vwap_today(stock: str) -> tuple[float | None, float | None]:
    """
    Returns (vwap, close_vs_vwap_pct) for today.
    VWAP = sum(typical_price * volume) / sum(volume)
    typical_price = (high + low + close) / 3
    """
    try:
        df = read_cache(stock, "15m", limit=50)
        if df is None or len(df) == 0:
            return None, None
        df.index = pd.to_datetime(df.index)
        today_df = df[df.index.date == _today()]
        if len(today_df) == 0:
            return None, None
        tp = (today_df["High"] + today_df["Low"] + today_df["Close"]) / 3
        total_vol = today_df["Volume"].sum()
        if total_vol == 0:
            return None, None
        vwap = float((tp * today_df["Volume"]).sum() / total_vol)
        close = float(today_df["Close"].iloc[-1])
        vs_vwap = round((close - vwap) / vwap * 100, 2) if vwap > 0 else None
        return round(vwap, 2), vs_vwap
    except Exception:
        return None, None

# ================================================================
# SECTOR UPDATER
# ================================================================
def update_sectors():
    """Download all sector indices and store in sector_cache."""
    log.info("Updating sector indices...")
    for name, sym in SECTOR_INDICES.items():
        for tf in ["1d", "1h"]:
            try:
                last = get_last_date(sym, tf)
                if last:
                    df = dl_since(sym, tf, last)
                else:
                    df = dl(sym, tf, "5y" if tf == "1d" else "2y")
                if df is None or len(df) == 0:
                    continue
                rows = []
                for idx, row in df.iterrows():
                    d = idx.isoformat() if hasattr(idx, "isoformat") else str(idx)[:19]
                    rows.append((name, sym, tf, d,
                                 float(row.get("Close", 0) or 0),
                                 float(row.get("Volume", 0) or 0)))
                con = _get_db()
                with _db_lock:
                    con.executemany(
                        "INSERT OR REPLACE INTO sector_cache "
                        "(sector,symbol,tf,date,close,volume) VALUES (?,?,?,?,?,?)",
                        rows
                    )
                    # Also write to price_cache so scanner can use dl_from_cache
                    con.executemany(
                        "INSERT OR REPLACE INTO price_cache "
                        "(stock,tf,date,open,high,low,close,volume) VALUES (?,?,?,?,?,?,?,?)",
                        [(sym, tf, r[3], 0, 0, 0, r[4], r[5]) for r in rows]
                    )
                    con.commit()
                write_cache(sym, tf, df)
                log.info(f"  {name} ({sym}) {tf}: {len(df)} bars")
            except Exception as e:
                log.warning(f"Sector {name} {tf}: {e}")

def get_sector_trend(sector_symbol: str, tf: str = "1d") -> str:
    """Return Stage2/Uptrend/Choppy/Bear for a sector index."""
    df = read_cache(sector_symbol, tf, limit=300)
    if df is None or len(df) < 50:
        return "Unknown"
    c = df["Close"].values.astype(float)
    ma50  = np.mean(c[-min(50, len(c)):])
    ma200 = np.mean(c[-min(200, len(c)):]) if len(c) >= 200 else ma50
    if c[-1] > ma50 > ma200:   return "Stage2"
    if c[-1] > ma200:          return "Uptrend"
    if c[-1] < ma50 < ma200:   return "Stage4"
    return "Choppy"

# ================================================================
# ONE STOCK — update all EOD timeframes
# ================================================================
def update_stock_eod(sym: str) -> dict:
    """Update 1d, 1wk, 1mo for one stock. Returns stats."""
    result = {"sym": sym, "ok": 0, "skip": 0, "err": 0}
    for tf in TF_EOD:
        try:
            last = get_last_date(sym, tf)
            if last:
                # Only download new bars since last stored date
                since = last[:10]   # date part only
                df = dl_since(sym, tf, since)
            else:
                df = dl(sym, tf, HISTORY[tf])

            if df is None or len(df) == 0:
                result["skip"] += 1
                continue
            n = write_cache(sym, tf, df)
            result["ok"] += 1
            log.debug(f"{sym} {tf}: +{n} bars")
        except Exception as e:
            log.debug(f"{sym} {tf} err: {e}")
            result["err"] += 1
    return result

# ================================================================
# ONE STOCK — update intraday timeframes
# ================================================================
def update_stock_intraday(sym: str) -> dict:
    """Update 15m + 1h for one stock. Returns stats.
    GAP5 FIX: was using dl(sym, tf, '7d') even when we had a last_date →
    redundantly re-downloading all 7d every 30min per stock.
    Now uses dl_since() for incremental updates, matching the EOD updater pattern.
    """
    result = {"sym": sym, "ok": 0, "skip": 0, "err": 0}
    for tf in TF_INTRADAY:
        try:
            last = get_last_date(sym, tf)
            if last:
                # Incremental: only fetch bars since last cached date
                df = dl_since(sym, tf, last)
            else:
                df = dl(sym, tf, HISTORY[tf])

            if df is None or len(df) == 0:
                result["skip"] += 1
                continue

            # Filter: only include COMPLETE bars (not the current forming bar)
            if tf == "15m":
                cutoff = last_complete_bar(15)
            elif tf == "1h":
                cutoff = last_complete_bar(60)
            else:
                cutoff = _now()
            df.index = pd.to_datetime(df.index)
            df = df[df.index <= cutoff]
            if len(df) == 0:
                result["skip"] += 1
                continue

            n = write_cache(sym, tf, df)
            result["ok"] += 1

            # Update RVOL profile from 15m data
            if tf == "15m":
                df_15m = read_cache(sym, "15m", limit=2000)
                if df_15m is not None:
                    update_rvol_profile(sym, df_15m)
        except Exception as e:
            log.debug(f"{sym} {tf} err: {e}")
            result["err"] += 1
    return result

# ================================================================
# GAP SCANNER — run at 9:20 AM IST
# ================================================================
def run_gap_scan(stocks: list[str]) -> list[dict]:
    """
    Scan for gap-ups > 3% using first 15min bar of the day.
    Compares today's open to prior day's close.
    """
    log.info(f"Gap scan: {len(stocks)} stocks")
    gaps = []

    def _check_gap(sym: str) -> dict | None:
        try:
            # Need today's first bar + yesterday's close
            df = dl(sym, "15m", "5d")
            if df is None or len(df) < 2:
                return None
            df.index = pd.to_datetime(df.index)
            today = _today()
            today_bars = df[df.index.date == today]
            prev_bars  = df[df.index.date < today]
            if len(today_bars) == 0 or len(prev_bars) == 0:
                return None
            first_open  = float(today_bars["Open"].iloc[0])
            prev_close  = float(prev_bars["Close"].iloc[-1])
            if prev_close <= 0:
                return None
            gap_pct = (first_open - prev_close) / prev_close * 100
            if gap_pct < 3.0:
                return None
            vol = float(today_bars["Volume"].iloc[0])
            return {"stock": sym.replace(".NS", ""), "gap_pct": round(gap_pct, 2),
                    "open_price": round(first_open, 2),
                    "prev_close": round(prev_close, 2),
                    "volume": vol}
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(_check_gap, s): s for s in stocks}
        for fut in as_completed(futs):
            r = fut.result()
            if r:
                gaps.append(r)

    gaps.sort(key=lambda x: -x["gap_pct"])
    log.info(f"Gap scan: {len(gaps)} gaps > 3%")

    # Store results
    if gaps:
        con = _get_db()
        today_str = str(_today())
        with _db_lock:
            for g in gaps:
                con.execute(
                    "INSERT OR REPLACE INTO gap_signals "
                    "(scan_date,stock,gap_pct,open_price,prev_close,volume) VALUES (?,?,?,?,?,?)",
                    (today_str, g["stock"], g["gap_pct"],
                     g["open_price"], g["prev_close"], g["volume"])
                )
            con.commit()
    return gaps

# ================================================================
# BATCH RUNNERS
# ================================================================
# ================================================================
# BHAVCOPY — NSE official EOD data (1 HTTP call = all stocks)
# ================================================================
def fetch_bhavcopy(trade_date=None) -> pd.DataFrame | None:
    """
    Download NSE sec_bhavdata_full CSV for given date.
    Returns DataFrame[stock, open, high, low, close, volume] — EQ series only.
    One HTTP call covers all 2100+ stocks. Zero rate-limit risk.
    """
    import requests as req
    if trade_date is None:
        trade_date = _today()
    date_str = trade_date.strftime("%d%m%Y")

    for url_tmpl in BHAVCOPY_URLS:
        url = url_tmpl.format(date=date_str)
        try:
            resp = req.get(url, timeout=30,
                           headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
            resp.raise_for_status()
            if len(resp.content) < 1000:   # empty / holiday file
                continue
            df = pd.read_csv(StringIO(resp.text))
            df.columns = df.columns.str.strip()

            # Filter EQ series only
            series_col = next((c for c in df.columns if "SERIES" in c.upper()), None)
            if series_col:
                df = df[df[series_col].str.strip() == "EQ"]

            # Normalise column names across NSE format versions
            col_map = {
                # symbol
                "SYMBOL":      "symbol",
                # OHLC
                "OPEN":        "open",  "OPEN_PRICE":  "open",
                "HIGH":        "high",  "HIGH_PRICE":  "high",
                "LOW":         "low",   "LOW_PRICE":   "low",
                "CLOSE":       "close", "CLOSE_PRICE": "close",
                # volume
                "TOTTRDQTY":   "volume", "TTL_TRD_QNTY": "volume",
                "TOTAL_TRADES":"trades",
            }
            df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

            required = {"symbol", "open", "high", "low", "close"}
            if not required.issubset(df.columns):
                log.warning(f"Bhavcopy {date_str} missing cols: {df.columns.tolist()}")
                continue

            df["stock"]  = df["symbol"].str.strip() + ".NS"
            df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0)
            for col in ("open", "high", "low", "close"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["open", "high", "low", "close"])

            log.info(f"Bhavcopy {date_str}: {len(df)} EQ stocks (source: {url})")
            return df[["stock", "open", "high", "low", "close", "volume"]].reset_index(drop=True)

        except Exception as e:
            log.warning(f"Bhavcopy {url}: {e}")

    return None


def resample_weekly_monthly(stocks: list[str]):
    """
    Derive 1wk and 1mo bars from accumulated 1d cache.
    Called after bhavcopy write so scanner gets all 3 EOD timeframes.
    No yfinance calls needed.
    """
    ok_wk = ok_mo = err = 0
    rules = [("1wk", "W-FRI"), ("1mo", "ME")]

    for sym in stocks:
        try:
            df_1d = read_cache(sym, "1d", limit=9999)
            if df_1d is None or len(df_1d) < 5:
                continue
            df_1d.index = pd.to_datetime(df_1d.index)

            for tf, rule in rules:
                try:
                    resampled = df_1d.resample(rule, label="left", closed="left").agg({
                        "Open": "first", "High": "max",
                        "Low":  "min",   "Close": "last",
                        "Volume": "sum",
                    }).dropna(subset=["Close"])
                    if len(resampled) == 0:
                        continue
                    # Incremental: only write bars newer than what's stored
                    last_stored = get_last_date(sym, tf)
                    if last_stored:
                        cutoff = pd.Timestamp(last_stored) - timedelta(days=40)
                        resampled = resampled[resampled.index >= cutoff]
                    if len(resampled) == 0:
                        continue
                    write_cache(sym, tf, resampled)
                    if tf == "1wk": ok_wk += 1
                    else:           ok_mo += 1
                except Exception as e:
                    log.debug(f"Resample {sym} {tf}: {e}")
                    err += 1
        except Exception as e:
            log.debug(f"resample_weekly_monthly {sym}: {e}")
            err += 1

    log.info(f"Resample done: 1wk={ok_wk} 1mo={ok_mo} err={err}")


def run_daily_bhavcopy(stocks: list[str]):
    """
    Daily EOD update via NSE Bhavcopy.

    Flow:
      1. Try today's bhavcopy → walk back up to 4 trading days (handles holidays)
      2. Bulk-write 1d bars to cache (single DB transaction)
      3. Resample 1wk + 1mo from accumulated 1d cache (no yfinance)
      4. Fallback: yfinance batch if bhavcopy unavailable

    HTTP calls: 1 (vs 6429 individual or ~129 batch yfinance calls).
    Rate-limit risk: zero for steps 1-3.
    """
    log.info(f"=== Daily update via NSE Bhavcopy {_ist()} ===")
    t0 = time.time()

    # Step 1: find most recent available bhavcopy
    bhav_df   = None
    bhav_date = None
    for days_back in range(6):
        check_date = _today() - timedelta(days=days_back)
        if check_date.weekday() >= 5:   # skip Sat/Sun
            continue
        bhav_df = fetch_bhavcopy(check_date)
        if bhav_df is not None:
            bhav_date = check_date
            break

    if bhav_df is None:
        log.error("Bhavcopy unavailable for last 6 days — falling back to yfinance batch")
        run_eod_update(stocks)
        return

    date_str = str(bhav_date)
    log.info(f"Writing {len(bhav_df)} stocks for {date_str}...")

    # Step 2: bulk-write 1d bars
    rows_to_insert = []
    already_current = 0

    # Build a set of stocks we track for fast lookup
    tracked = set(stocks)

    for _, row in bhav_df.iterrows():
        sym = row["stock"]
        if sym not in tracked:
            continue                        # not in our universe
        last = get_last_date(sym, "1d")
        if last and last[:10] >= date_str:
            already_current += 1
            continue                        # already have today's bar
        rows_to_insert.append((
            sym, "1d", date_str,
            float(row["open"]), float(row["high"]),
            float(row["low"]),  float(row["close"]),
            float(row["volume"]),
        ))

    written = 0
    if rows_to_insert:
        con = _get_db()
        with _db_lock:
            con.executemany(
                "INSERT OR REPLACE INTO price_cache "
                "(stock,tf,date,open,high,low,close,volume) VALUES (?,?,?,?,?,?,?,?)",
                rows_to_insert,
            )
            # Update cache_meta for written stocks
            for r in rows_to_insert:
                sym = r[0]
                bar_count = con.execute(
                    "SELECT COUNT(*) FROM price_cache WHERE stock=? AND tf='1d'", (sym,)
                ).fetchone()[0]
                con.execute(
                    "INSERT OR REPLACE INTO cache_meta "
                    "(stock,tf,last_date,last_updated,bar_count) VALUES (?,?,?,?,?)",
                    (sym, "1d", date_str, str(_today()), bar_count),
                )
            con.commit()
        written = len(rows_to_insert)

    log.info(f"1d: wrote={written} already_current={already_current} "
             f"not_in_universe={len(bhav_df)-written-already_current} "
             f"| {time.time()-t0:.1f}s")

    # Step 3: resample 1wk + 1mo (no yfinance needed)
    log.info("Resampling 1wk + 1mo from 1d cache...")
    resample_weekly_monthly(stocks)

    log.info(f"Daily bhavcopy complete: {time.time()-t0:.1f}s")


def run_eod_update(stocks: list[str]):
    """
    Update 1d/1wk/1mo for all stocks using METHOD4 batch downloads.

    Instead of 2143 stocks × 3 TFs = 6429 individual HTTP calls,
    we batch 50 symbols per call → ~43 calls per TF → ~129 total.
    Groups stocks by their last_date so each batch uses a single
    start= or period= argument.
    """
    # METHOD2+3: shuffle to break alphabetical pattern Yahoo fingerprints
    stocks = list(stocks)
    random.shuffle(stocks)

    log.info(f"EOD update (batch): {len(stocks)} stocks × {TF_EOD} "
             f"| batch={BATCH_SIZE} workers={MAX_WORKERS}")
    t0 = time.time()
    total_ok = total_skip = total_err = 0

    for tf in TF_EOD:
        log.info(f"  ── TF {tf} ──")
        tf_ok = tf_skip = tf_err = 0

        # Group stocks by their last cached date so each batch can share
        # a single start= value (stocks updated the same day go in one call)
        date_groups: dict[str | None, list[str]] = {}
        for sym in stocks:
            last = get_last_date(sym, tf)
            since = last[:10] if last else None
            date_groups.setdefault(since, []).append(sym)

        log.info(f"    {len(date_groups)} date-groups, "
                 f"{sum(len(v) for v in date_groups.values())} stocks")

        for since_date, group_syms in date_groups.items():
            label = since_date or f"full ({HISTORY[tf]})"
            n_batches = (len(group_syms) + BATCH_SIZE - 1) // BATCH_SIZE
            log.info(f"    since={label}: {len(group_syms)} stocks "
                     f"→ {n_batches} batches")

            for i in range(0, len(group_syms), BATCH_SIZE):
                batch = group_syms[i : i + BATCH_SIZE]

                # Outer retry: catches RateLimitExhausted when dl_batch exhausts inner retries
                # Waits 2min → 3min → 5min → gives up (probably all delisted/symbol error)
                OUTER_WAITS = [120, 180, 300]  # seconds between outer retries
                data = {}
                for outer_attempt, wait_sec in enumerate([0] + OUTER_WAITS):
                    if wait_sec > 0:
                        log.warning(f"  ⛔ Batch {tf} outer retry {outer_attempt}/{len(OUTER_WAITS)} "
                                    f"— waiting {wait_sec//60}min before retry...")
                        _reset_session()
                        time.sleep(wait_sec)
                    try:
                        if since_date:
                            data = dl_batch(batch, tf, since_date=since_date)
                        else:
                            data = dl_batch(batch, tf, period=HISTORY[tf])
                        break  # success — exit outer retry loop
                    except RateLimitExhausted:
                        if outer_attempt < len(OUTER_WAITS):
                            continue  # go to next outer wait
                        else:
                            log.error(f"  ❌ Batch {tf} [{i}:{i+BATCH_SIZE}] "
                                      f"gave up after {len(OUTER_WAITS)} outer retries — skipping")
                            data = {}
                    except Exception as e:
                        log.warning(f"Batch {tf} {label} [{i}:{i+BATCH_SIZE}]: {e}")
                        data = {}
                        break

                for sym, df in data.items():
                    try:
                        n = write_cache(sym, tf, df)
                        tf_ok += 1
                        log.debug(f"{sym} {tf}: +{n} bars")
                    except Exception as e:
                        log.debug(f"{sym} {tf} write err: {e}")
                        tf_err += 1

                tf_skip += len(batch) - len(data)

                # Progress log every 500 stocks processed
                processed = i + len(batch)
                if processed % 500 == 0 or processed >= len(group_syms):
                    elapsed = time.time() - t0
                    log.info(f"    {tf} {label}: {processed}/{len(group_syms)} | "
                             f"ok={tf_ok} skip={tf_skip} err={tf_err} | "
                             f"{elapsed:.0f}s elapsed")

        log.info(f"  {tf} complete: ok={tf_ok} skip={tf_skip} err={tf_err}")
        total_ok += tf_ok; total_skip += tf_skip; total_err += tf_err

    log.info(f"EOD done: {time.time()-t0:.1f}s | "
             f"ok={total_ok} skip={total_skip} err={total_err}")

def run_intraday_update(stocks: list[str]):
    """Update 15m + 1h for watchlist stocks."""
    log.info(f"Intraday update: {len(stocks)} stocks × {TF_INTRADAY}")
    t0 = time.time()
    ok = err = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:  # MAX_WORKERS=5 (not 10 — rate-limit safe)
        futs = {ex.submit(update_stock_intraday, s): s for s in stocks}
        for fut in as_completed(futs):
            try:
                r = fut.result()
                ok += r["ok"]; err += r["err"]
            except Exception:
                err += 1

    log.info(f"Intraday done: {time.time()-t0:.1f}s | ok={ok} err={err}")

# ================================================================
# BOOTSTRAP — full download, run once
# ================================================================
def run_bootstrap(stocks: list[str]):
    """
    Full historical download for all stocks and all timeframes.
    Run once manually. After this, --daily and --intraday only fetch diffs.

    Timeline estimate (2000 stocks):
      1d/1wk/1mo:  ~15-20 min
      1h:          ~15-20 min
      15m:         ~20-25 min (60d limit, still 2000 stocks)
      Total:       ~55-65 min (first time only)
    """
    log.info(f"=== BOOTSTRAP: {len(stocks)} stocks, all timeframes ===")
    log.info("This runs once. Estimated time: 55-65 min.")

    # Phase 1: EOD
    log.info("Phase 1/3: EOD data (1d, 1wk, 1mo)...")
    run_eod_update(stocks)

    # Phase 2: Hourly
    log.info("Phase 2/3: Hourly data (1h, 2yr)...")
    t0 = time.time()
    ok = err = skip = 0

    def _fetch_1h(sym):
        # Skip if already have 1h data
        existing = get_last_date(sym, "1h")
        if existing:
            return -1  # already done
        df = dl(sym, "1h", "2y")
        if df is not None and len(df) > 0:
            return write_cache(sym, "1h", df)
        return 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(_fetch_1h, s): s for s in stocks}
        done = 0
        for fut in as_completed(futs):
            done += 1
            try:
                n = fut.result()
                if n == -1: skip += 1
                elif n: ok += 1
                else: err += 1
            except Exception:
                err += 1
            if done % 200 == 0:
                log.info(f"  1h: {done}/{len(stocks)} | ok={ok} skip={skip} err={err}")

    log.info(f"Phase 2 done: {time.time()-t0:.1f}s")

    # Phase 3: 15min (60d limit per call)
    log.info("Phase 3/3: 15min data (60d limit per call)...")
    t0 = time.time()
    ok = err = 0

    def _fetch_15m(sym):
        # Skip if already have 15m data (allows resume after timeout)
        existing = get_last_date(sym, "15m")
        if existing:
            return -1  # already done
        df = dl(sym, "15m", "60d")
        if df is None or len(df) == 0:
            return 0
        n = write_cache(sym, "15m", df)
        if n > 0:
            update_rvol_profile(sym, df)
        return n

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(_fetch_15m, s): s for s in stocks}
        done = 0
        for fut in as_completed(futs):
            done += 1
            try:
                n = fut.result()
                if n: ok += 1
                else: err += 1
            except Exception:
                err += 1
            if done % 200 == 0:
                log.info(f"  15m: {done}/{len(stocks)} | ok={ok} err={err}")

    log.info(f"Phase 3 done: {time.time()-t0:.1f}s")

    # Sector indices
    update_sectors()

    log.info("Bootstrap complete. Run --daily and --intraday from now on.")

# ================================================================
# SEND TELEGRAM (optional — gap alerts)
# ================================================================
def send_telegram(msg: str):
    tg_token = os.environ.get("TG_BOT_TOKEN", "")
    tg_chat  = os.environ.get("TG_CHAT_ID", "")
    if not tg_token or not tg_chat:
        return
    try:
        import requests as req
        if len(msg) > 4000:
            msg = msg[:3990] + "\n..."
        req.post(
            f"https://api.telegram.org/bot{tg_token}/sendMessage",
            data={"chat_id": tg_chat, "text": msg, "parse_mode": "HTML"},
            timeout=15,
        )
    except Exception as e:
        log.error(f"Telegram: {e}")

def format_gap_alert(gaps: list[dict]) -> str:
    if not gaps:
        return ""
    lines = [f"<b>\U0001f4c8 Gap Scanner — {_ist()} ({len(gaps)} gaps &gt;3%)</b>\n"]
    for g in gaps[:20]:
        em = "\U0001f7e2" if g["gap_pct"] >= 5 else "\U0001f7e1"
        lines.append(
            f"{em} <b>{g['stock']}</b> +{g['gap_pct']}%\n"
            f"   Open \u20b9{g['open_price']} | Prev close \u20b9{g['prev_close']}"
        )
    return "\n".join(lines)

# ================================================================
# CACHE STATS
# ================================================================
def print_stats():
    con = _get_db()
    rows = con.execute("""
        SELECT tf, count(distinct stock) stocks, count(*) bars, 
               min(date) oldest, max(date) newest
        FROM price_cache
        GROUP BY tf ORDER BY tf
    """).fetchall()
    print(f"\n{'TF':<8} {'Stocks':>8} {'Bars':>12} {'Oldest':<14} {'Newest':<14}")
    print("-" * 60)
    for r in rows:
        print(f"{r[0]:<8} {r[1]:>8,} {r[2]:>12,} {str(r[3]):<14} {str(r[4]):<14}")
    rvol = con.execute("SELECT count(distinct stock) FROM rvol_profile").fetchone()[0]
    gaps = con.execute("SELECT count(*) FROM gap_signals").fetchone()[0]
    print(f"\nRVOL profiles: {rvol} stocks")
    print(f"Gap signals:   {gaps} records")
    db_mb = os.path.getsize(CACHE_PATH) / 1e6
    print(f"Cache size:    {db_mb:.1f} MB")

# ================================================================
# MAIN
# ================================================================
def main():
    ap = argparse.ArgumentParser(description="NSE Data Updater")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--bootstrap",  action="store_true", help="Full download (run once)")
    mode.add_argument("--daily",      action="store_true", help="Append new EOD bars")
    mode.add_argument("--intraday",   action="store_true", help="Append new 15m/1h bars")
    mode.add_argument("--gap",        action="store_true", help="Gap scanner (9:20 AM)")
    mode.add_argument("--sectors",    action="store_true", help="Update sector indices")
    mode.add_argument("--stats",      action="store_true", help="Print cache statistics")
    ap.add_argument("--telegram",     action="store_true")
    args = ap.parse_args()

    if args.stats:
        print_stats()
        return

    # Initialise DB (creates tables / runs migration)
    _get_db()

    if args.sectors:
        update_sectors()
        return

    if args.gap:
        log.info(f"=== Gap scan {_ist()} ===")
        stocks = load_universe()
        gaps   = run_gap_scan(stocks)
        if args.telegram and gaps:
            send_telegram(format_gap_alert(gaps))
        return

    if args.intraday:
        log.info(f"=== Intraday update {_ist()} ===")
        # Update watchlist stocks + top 300 for quick-scan coverage
        wl_stocks = load_watchlist()
        # Add Nifty 500 fallback for coverage even without watchlist
        try:
            universe = load_universe()
            all_stocks = list(set(wl_stocks) | set(universe[:300]))
        except Exception:
            all_stocks = wl_stocks
        log.info(f"Stocks: {len(wl_stocks)} watchlist + top 300 = {len(all_stocks)} total")
        run_intraday_update(all_stocks)
        return

    if args.daily:
        log.info(f"=== Daily EOD update {_ist()} ===")
        stocks = load_universe()
        run_daily_bhavcopy(stocks)   # 1 HTTP call vs 6429; yfinance fallback if unavailable
        update_sectors()
        return

    if args.bootstrap:
        stocks = load_universe()
        run_bootstrap(stocks)
        return

if __name__ == "__main__":
    main()
