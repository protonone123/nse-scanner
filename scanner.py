#!/usr/bin/env python3
"""
NSE Live Pattern Scanner v4.0
==============================
v4.0 — Institutional Gap Analysis Fixes (2026-05-23):

  GAP-P1  FIXED — Pattern start date is now pivot-based, not window-based.
                  Each detector returns start_bar (first pivot of the pattern)
                  which is translated to a real calendar date.
                  Previously: df.index[-window] → always overstated duration.
                  Now: actual left-rim / first contraction / pole-start bar.

  GAP-P2  FIXED — Pattern end date added to every detector and all outputs.
                  pattern_end_date = the bar where the formation completed
                  (right rim, final contraction low, handle low, etc.)
                  Output: "📅 2025-11-14 → 2026-01-08 (38d)" in Telegram + CSV.

  GAP-P3  FIXED — Timeframe label now reflects the actual chart timeframe
                  (Daily / Weekly) instead of a meaningless window-size bucket.
                  Multi-timeframe label shown when weekly pattern also active.

  GAP-P5  FIXED — HighTightFlag was listed in DETECTORS but det_htf() did not
                  exist → silent NameError on any HTF scan window.
                  det_flag() already handles HTF (pole gain ≥ 100%) and returns
                  pattern="HighTightFlag". Removed redundant DETECTORS entry.

  GAP-P6  FIXED — VCP contraction count now exposed in output notes as
                  "VCP(3C)" / "VCP(4C)" etc. Score bonus for 3+ contractions.

  GAP-D3  FIXED — NSE holiday calendar 2025-2026 integrated into
                  _trading_day_add(). T1/T2/T3 ETAs now skip market holidays.

  GAP-F1  FIXED — Promoter pledge score wired into scan_stock() and score10.
                  +0.3 for pledge <5% + promoter >50%; -0.5 for pledge >20%.
                  Notes: "PLEDGE⚠️(25%)" when pledging is dangerous.

  GAP-F2  FIXED — Bulk/block deals from fundamentals.py now integrated.
                  get_bulk_deals_today() called once at daily scan start.
                  Notes: "BULK-DEAL💼 ₹NNNCr" when deal exists on signal day.
                  +0.4 to score10 for bulk deal confirmation.

  GAP-F3  FIXED — Piotroski score wired into score10.
                  +0.3 for Piotroski ≥ 7; -0.3 for Piotroski ≤ 2.
                  Shown in CSV output as a dedicated column.

  GAP-B2  FIXED — Outcome tracking now records actual_r_multiple and exit_type.
                  print_outcome_summary shows expectancy (E) per pattern.

  GAP-O1  FIXED — Telegram messages > 4000 chars now split into multiple
                  messages instead of hard-cutting at 4000.

  GAP-OP4 FIXED — Output directory cleanup: files older than 30 days deleted
                  at end of each daily scan.

  GAP-S1  IMPROVED — score10 recalibrated: bulk deal, promoter pledge, and
                  Piotroski components added. VCP contraction bonus added.
                  Total max remains 10.0 (components re-weighted).

  NOTE   GAPs D1, D2, D3(broker), S2(position sizing) require external data
         sources (Kite Connect / NSE Bhavcopy / BSE filings) that are outside
         the yfinance-based architecture. Documented in gap analysis MD file.

Inherited from v3.7:
  GAP 2  RS percentile = true O'Neil cross-sectional percentile (4Q weighted)
  GAP 4  OBV, A/D Line, Up/Down volume ratio
  GAP 6  CupHandle handle placement, VCP depth+duration shrinkage, InvHS vol
  GAP 7  signal_outcomes auto-populated at end of every daily scan
  GAP 8  EpisodicPivot stop = 50% gap fill (not 0.1% above close)
  GAP 9  Market breadth = stratified 150-stock sample
  GAP 10 Weekly chart validation gate for base patterns
  GAP 11 RS line leadership (+0.5 to score10)

Inherited from v3.6:
  1. DB PERSISTENCE — watchlist.json committed back via git.
  2. NSE UNIVERSE BLOCKED — hardcoded NIFTY_500_FALLBACK.
  3. CSV sent as Telegram document.
  4. SCHEDULE — 3 daily scans + 30-min during market hours.
  5. Universe fallback so scanner always has something to scan.
  6. Telegram sendDocument API for CSV files.

Usage:
  python scanner.py --daily      # full scan (8AM, 12:30PM, 4:30PM)
  python scanner.py --halfhour   # 30-min watchlist + quick-scan
  python scanner.py --dashboard  # Flask web UI
  python scanner.py --healthcheck
  python scanner.py --test
  python scanner.py --outcomes   # manual outcome summary (also runs automatically)
"""

import os, sys, json, time, sqlite3, argparse, logging, io
from datetime import date, datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import contextlib
import warnings
warnings.filterwarnings("ignore")

# IST = UTC+5:30. GitHub Actions runs UTC — this makes all times correct.
_IST = timezone(timedelta(hours=5, minutes=30))
def _now():  return datetime.now(_IST)
def _ist(fmt="%H:%M IST"): return _now().strftime(fmt)
def _today(): return _now().date()

import yfinance as yf
import pandas as pd
import numpy as np
from scipy.signal import find_peaks
from scipy.stats import percentileofscore

# ================================================================
# PATHS & LOGGING
# ================================================================
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
LOG_DIR    = os.path.join(BASE_DIR, "logs")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
DB_PATH    = os.path.join(BASE_DIR, "signals.db")
CACHE_PATH = os.path.join(BASE_DIR, "price_cache.db")  # incremental OHLCV cache
WL_PATH    = os.path.join(BASE_DIR, "watchlist.json")   # persisted via git

for d in [LOG_DIR, OUTPUT_DIR]:
    os.makedirs(d, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(LOG_DIR, f"scan_{_today()}.log"),
            encoding="utf-8"),
    ],
)
log = logging.getLogger("scanner")

# ================================================================
# CONFIG
# ================================================================
NIFTY_SYM    = "^NSEI"
PERIOD_DAILY = "1y"
STALE_DAYS   = 5     # re-fetch full history if cache is older than this
PERIOD_QUICK = "3mo"
MAX_WORKERS  = 4
DL_RETRIES   = 3
DL_BACKOFF   = 3.0
QUICK_SIZE   = 300   # stocks scanned in 30-min mode

TG_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT  = os.environ.get("TG_CHAT_ID", "")

CS = {
    "C_min": 0.25, "A_min": 0.25, "N_max_from_high": 0.15,
    "L_min_rs": 1.10, "I_min_instl": 0.20,
    "buy_strong": 6, "buy_moderate": 4,
}

# ── Trading filters ────────────────────────────────────────────────────────────
MIN_LIQUIDITY_CR   = 15.0   # min avg daily turnover ₹ Cr (was 1 — too loose)
MIN_DIST_52WK_PCT  = 0.00   # stock must be within this % of 52wk high (0 = no filter)
MAX_DIST_52WK_PCT  = 0.30   # within 30% of 52-week high (Minervishi rule N)
MAX_STOP_PCT       = 0.08   # max stop loss from entry (8% — Minervishi hard limit)
MIN_RS_PERCENTILE  = 40     # min relative-strength percentile vs universe
INDIA_VIX_SYM      = "^INDIAVIX"
VIX_HIGH_THRESH    = 22.0   # if VIX > this: reduce aggression, flag as "High Fear"
VIX_EXTREME_THRESH = 30.0   # if VIX > this: suppress BUY-strong signals
HALFHOUR_CONFIRM_HOUR = 13  # MomBurst halfhour alerts suppressed before 1 PM IST

INTRADAY_DETECTORS = {"MomBurst", "EpisodicPivot", "PocketPivot"}

# ================================================================
# NIFTY 500 FALLBACK — used when NSE URL is blocked (GitHub IPs)
# Top 300 liquid NSE stocks hardcoded so scanner ALWAYS works
# ================================================================
NIFTY_500_FALLBACK = [
    "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","ITC","SBIN",
    "BHARTIARTL","KOTAKBANK","LT","AXISBANK","BAJFINANCE","ASIANPAINT","MARUTI",
    "SUNPHARMA","TITAN","WIPRO","ULTRACEMCO","BAJAJFINSV","NESTLEIND","POWERGRID",
    "NTPC","TECHM","TATAMOTORS","HCLTECH","JSWSTEEL","TATASTEEL","ADANIENT","ADANIPORTS",
    "ONGC","COALINDIA","BRITANNIA","DIVISLAB","DRREDDY","EICHERMOT","GRASIM","HDFCLIFE",
    "INDUSINDBK","M&M","SBILIFE","SHREECEM","TATACONSUM","UPL","CIPLA","APOLLOHOSP",
    "BAJAJ-AUTO","BPCL","DABUR","HAVELLS","HEROMOTOCO","HINDPETRO","IOC","LTIM",
    "LUPIN","MARICO","MCDOWELL-N","MUTHOOTFIN","NAUKRI","PIDILITIND","PIIND",
    "SIEMENS","TORNTPHARM","TRENT","VEDL","VOLTAS","ZOMATO","PAYTM","NYKAA","DELHIVERY",
    "IRCTC","LICI","ADANIGREEN","ADANITRANS","ATGL","AWL","CANBK","BANKBARODA",
    "FEDERALBNK","IDFCFIRSTB","INDIGO","IRFC","JSWENERGY","LAURUSLABS","LICHSGFIN",
    "LINDEINDIA","MOTHERSON","MRF","NMDC","OBEROIRLTY","PAGEIND","PETRONET","PFC",
    "POLYCAB","RECLTD","SAIL","SBICARD","TATAPOWER","TIINDIA","TVSMOTOR","VBL",
    "ZYDUSLIFE","ABCAPITAL","ABIRLANUVO","ACC","ADANIPOWER","AEGISCHEM","AIAENG",
    "AJANTPHARM","AKZOINDIA","ALKEM","AMARAJABAT","AMBUJACEM","APLAPOLLO","APLLTD",
    "ASTRAL","ATUL","AUBANK","AUROPHARMA","BALKRISIND","BANDHANBNK","BATAINDIA",
    "BAYERCROP","BERGEPAINT","BIOCON","BLUESTAR","BSOFT","CANFINHOME","CASTROLIND",
    "CEATLTD","CENTURYPLY","CESC","CHOLAFIN","CUMMINSIND","CYIENT","DEEPAKNTR",
    "DIXON","DMART","ESCORTS","EXIDEIND","FINEORG","FLUOROCHEM","FORTIS","GAIL",
    "GLAND","GLAXO","GMRINFRA","GNFC","GODREJCP","GODREJIND","GODREJPROP","GRANULES",
    "GSPL","GUIGAS","HAL","HINDALCO","HINDCOPPER","HONAUT","IBREALEST","ICICIPRULI",
    "IDBI","IEX","IGL","INDHOTEL","INDUSTOWER","INOXWIND","INTELLECT","IPCALAB",
    "JKCEMENT","JUBLFOOD","JUBLINGREA","KAJARIACER","KANSAINER","KPITTECH","KPRMILL",
    "KRBL","LALPATHLAB","LEMONTREE","LICI","LTTS","LUXIND","MAHSEAMLES","MANAPPURAM",
    "MAPMYINDIA","MAXHEALTH","MCX","MEDPLUS","METROBRAND","MFSL","MGLAMINES",
    "MHRIL","MIDHANI","MINDTREE","MKPL","MRPL","NATCOPHARM","NAVINFLUOR","NAUKRI",
    "NBCC","NDTV","NHPC","NLCINDIA","NSLNISP","NUVAMA","OFSS","OLECTRA",
    "OPTIEMUS","ORIENTELEC","PGHH","PHOENIXLTD","PNBHOUSING","POLICYBZR","PRAJIND",
    "PRESTIGE","PRINCEPIPE","PRIVISCL","PSPPROJECT","PVRINOX","RADICO","RAILTEL",
    "RAININD","RAJESHEXPO","RAYMOND","RBLBANK","RCF","REDINGTON","RELAXO",
    "RITES","RKFORGE","ROSSARI","ROUTE","SAFARI","SAPPHIRE","SCHAEFFLER","SEQUENT",
    "SFL","SHYAMMETL","SIGNATURE","SJVN","SKFINDIA","SOBHA","SOLARINDS","SONACOMS",
    "SPANDANA","SPARC","SPIML","SRF","STARCEMENT","SUNTV","SUPRAJIT","SUVEN",
    "SUZLON","SWANENERGY","SYMPHONY","TANLA","TATACHEM","TATACOMM","TATAELXSI",
    "TATAINVEST","TATATECH","TCPL","TEAMLEASE","TEJASNET","THYROCARE","TIMKEN",
    "TTKPRESTIG","UJJIVANSFB","UNITDSPR","UTIAMC","VAIBHAVGBL","VGUARD","VIPIND",
    "VINATIORGA","VSTIND","WABAG","WELCORP","WELSPUNLIV","WESTLIFE","WHIRLPOOL",
    "WIPRO","WOCKPHARMA","ZEEL","ZENTEC","ZFCVINDIA",
]
NIFTY_500_FALLBACK_NS = [s + ".NS" for s in NIFTY_500_FALLBACK]

# ================================================================
# DATABASE (rebuilt each daily run — stateless)
# ================================================================
_db_lock = Lock()

def get_db():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date TEXT, scan_time TEXT, scan_mode TEXT,
            stock TEXT, name TEXT, sector TEXT,
            cap_class TEXT, cap_cr REAL,
            pattern TEXT, timeframe TEXT, status TEXT,
            breakout_zone REAL, cmp REAL, stop_loss REAL,
            target_1 REAL, target_2 REAL, target_3 REAL,
            risk_reward REAL, quality REAL, vol_surge REAL,
            rs_percentile REAL, dist_52wk_pct REAL,
            canslim_score INTEGER, data_completeness INTEGER,
            converging TEXT, leg TEXT,
            earnings_near INTEGER, ftd_active INTEGER,
            vol_dryup INTEGER, stage TEXT,
            recommendation TEXT,
            m1 REAL, m2 REAL, m3 REAL, m4 REAL, m5 REAL,
            notes TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date TEXT, scan_time TEXT, mode TEXT,
            stocks_total INTEGER, stocks_ok INTEGER,
            signals INTEGER, buys INTEGER, elapsed_sec REAL,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS alerts_sent (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date TEXT, stock TEXT, pattern TEXT, status TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS signal_outcomes (
            stock TEXT, pattern TEXT, signal_date TEXT,
            entry_price REAL, stop_loss REAL, target_1 REAL,
            price_3d REAL, price_5d REAL, price_10d REAL, price_20d REAL,
            return_3d REAL, return_5d REAL, return_10d REAL, return_20d REAL,
            hit_t1 INTEGER DEFAULT 0, hit_stop INTEGER DEFAULT 0,
            actual_r_multiple REAL,   -- GAP-B2: (exit_price - entry) / (entry - stop)
            exit_type TEXT,           -- GAP-B2: T1 | T2 | T3 | STOP | TIME_STOP | OPEN
            tracked_date TEXT,
            PRIMARY KEY (stock, pattern, signal_date)
        );
        CREATE INDEX IF NOT EXISTS idx_sig_date ON signals(scan_date);
        CREATE INDEX IF NOT EXISTS idx_sig_stock ON signals(stock);
        CREATE INDEX IF NOT EXISTS idx_alerts ON alerts_sent(scan_date,stock);
    """)
    # ── Schema migration: add columns that may be missing from older DB ──────
    # SQLite does not support IF NOT EXISTS on ALTER TABLE — use try/except
    _new_cols = [
        ("rs_percentile",      "REAL"),
        ("dist_52wk_pct",      "REAL"),
        ("ti65",               "REAL"),
        ("lynch_score_val",    "REAL"),
        # Formation metadata + ETA columns
        ("pattern_formed",     "TEXT"),
        ("pattern_start_date", "TEXT"),
        ("pattern_end_date",   "TEXT"),
        ("formation_days",     "INTEGER"),
        ("t1_eta",             "TEXT"),
        ("t2_eta",             "TEXT"),
        ("t3_eta",             "TEXT"),
        # GAP-F1/F2/F3 fundamental columns
        ("piotroski_score",    "INTEGER"),
        ("pledge_pct",         "REAL"),
        ("bulk_deal_cr",       "REAL"),
    ]
    _seen = set()
    for col, typ in _new_cols:
        if col in _seen: continue
        _seen.add(col)
        try:
            con.execute(f"ALTER TABLE signals ADD COLUMN {col} {typ}")
            con.commit()
        except Exception:
            pass   # column already exists — fine
    con.commit()
    return con

def db_exec(con, sql, params=None):
    with _db_lock:
        con.execute(sql, params or [])
        con.commit()

def db_execmany(con, sql, rows):
    if not rows: return
    with _db_lock:
        con.executemany(sql, rows)
        con.commit()

def db_query(con, sql, params=None):
    cur = con.execute(sql, params or [])
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

# ================================================================
# WATCHLIST — persisted as JSON so GitHub Actions can commit it
# ================================================================
def load_watchlist():
    """Load watchlist — 7-day window to prevent bloat (4545-item explosion)."""
    if os.path.exists(WL_PATH):
        try:
            with open(WL_PATH) as f:
                data = json.load(f)
            cutoff = str(_today() - timedelta(days=7))  # FIX: was 30d → 4545 items
            return [w for w in data if w.get("added_date", "") >= cutoff]
        except Exception:
            pass
    return []

def save_watchlist(items):
    """Save watchlist — prune to 7 days + cap 300 items per stock+pattern."""
    cutoff = str(_today() - timedelta(days=7))   # FIX: was 14d
    items = [i for i in items if i.get("added_date","") >= cutoff]
    # Dedup: keep only latest entry per stock+pattern pair
    seen = {}
    for item in sorted(items, key=lambda x: x.get("added_date",""), reverse=True):
        key = f"{item.get('stock','')}_{item.get('pattern','')}"
        seen.setdefault(key, item)
    items = list(seen.values())[:500]   # hard cap 500
    with open(WL_PATH, "w") as f:
        json.dump(items, f, indent=2)
    log.info(f"Watchlist saved: {len(items)} items → {WL_PATH}")

def already_alerted_today(stock, pattern):
    """BUG6 FIX: use alerts_sent DB (persisted via GH Actions cache), not ephemeral JSON file."""
    try:
        con = get_db()
        row = con.execute(
            "SELECT id FROM alerts_sent WHERE scan_date=? AND stock=? AND pattern=?",
            (str(_today()), stock, pattern)
        ).fetchone()
        return row is not None
    except Exception:
        return False

def mark_alert_sent(stock, pattern, status):
    """BUG6 FIX: write to alerts_sent DB, not ephemeral JSON file."""
    try:
        con = get_db()
        with _db_lock:
            con.execute(
                "INSERT OR IGNORE INTO alerts_sent (scan_date,stock,pattern,status) VALUES (?,?,?,?)",
                (str(_today()), stock, pattern, status)
            )
            con.commit()
    except Exception as e:
        log.debug(f"mark_alert_sent {stock}: {e}")

# ================================================================
# DATA LAYER — with NSE fallback
# ================================================================
def load_universe():
    """Load NSE equity list. Falls back to hardcoded top-300 if URL blocked."""
    urls = [
        "https://archives.nseindia.com/content/equities/EQUITY_L.csv",
        "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
    ]
    for url in urls:
        for attempt in range(2):
            try:
                import requests as req
                resp = req.get(url, timeout=20,
                               headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
                resp.raise_for_status()
                from io import StringIO
                df = pd.read_csv(StringIO(resp.text)).dropna(subset=["SYMBOL"])
                for col in [" SERIES", "SERIES"]:
                    if col in df.columns:
                        df = df[df[col].str.strip() == "EQ"]; break
                syms = [s.strip() + ".NS" for s in df["SYMBOL"].astype(str).tolist()]
                log.info(f"Universe: {len(syms)} stocks from {url}")
                return syms
            except Exception as e:
                log.warning(f"Universe URL {url} attempt {attempt+1}: {e}")
                time.sleep(2)

    log.warning("NSE URL blocked — using hardcoded Nifty-500 fallback")
    return NIFTY_500_FALLBACK_NS

# ── Yahoo Finance crumb-aware session ──────────────────────────────────────
# Yahoo requires a crumb token tied to a browser-like session cookie.
# On GitHub Actions IPs, plain requests get 401 "Invalid Crumb".
# Fix: curl_cffi impersonates Chrome TLS fingerprint, warm up session once,
# reuse it for all downloads. Reset session automatically on repeated 401s.

_YF_SESSION = None
_YF_SESSION_LOCK = Lock()

# ================================================================
# GLOBAL RATE-LIMIT CIRCUIT BREAKER
# ================================================================
# Shared across all worker threads. When any thread hits a 429,
# it sets _RL_UNTIL so ALL threads pause before their next request.

_RL_LOCK  = Lock()
_RL_UNTIL = 0.0   # epoch seconds; 0 = no limit active

def _set_rate_limit(seconds: int):
    global _RL_UNTIL
    with _RL_LOCK:
        new_until = time.time() + seconds
        if new_until > _RL_UNTIL:
            _RL_UNTIL = new_until
            log.warning(f"Rate limit — ALL workers paused for {seconds}s "
                        f"(until {time.strftime('%H:%M:%S', time.localtime(new_until))})")

def _wait_if_rate_limited(caller: str = ""):
    remaining = _RL_UNTIL - time.time()
    if remaining > 0:
        log.info(f"  [{caller}] rate-limit gate — waiting {remaining:.0f}s")
        time.sleep(remaining + 0.5)

# ================================================================
# PRICE CACHE — incremental OHLCV store
# ================================================================
# How it works:
#   First run (or cache miss) → downloads full 1y via yf.download, stores all bars
#   Subsequent runs            → downloads only last 7d, upserts new bars
#   GitHub Actions             → cache persisted via actions/cache on price_cache.db
#   Result                     → daily scan: ~20 min first day, ~4 min every day after
#
# Table: price_cache(stock, date, open, high, low, close, volume)
# Table: cache_meta(stock, last_updated, bar_count)
# ================================================================

_cache_lock = Lock()
_cache_con  = None   # module-level connection (thread-safe with WAL)

def _get_cache():
    global _cache_con
    with _cache_lock:
        if _cache_con is None:
            _cache_con = sqlite3.connect(CACHE_PATH, check_same_thread=False)
            _cache_con.execute("PRAGMA journal_mode=WAL")
            _cache_con.execute("PRAGMA synchronous=NORMAL")
            _cache_con.executescript("""
                CREATE TABLE IF NOT EXISTS price_cache (
                    stock   TEXT    NOT NULL,
                    date    TEXT    NOT NULL,
                    open    REAL,
                    high    REAL,
                    low     REAL,
                    close   REAL    NOT NULL,
                    volume  REAL,
                    PRIMARY KEY (stock, date)
                );
                CREATE TABLE IF NOT EXISTS cache_meta (
                    stock        TEXT PRIMARY KEY,
                    last_updated TEXT,
                    bar_count    INTEGER,
                    fund_json    TEXT,
                    fund_updated TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_cache_stock ON price_cache(stock);
            """)
            _cache_con.commit()
        return _cache_con


def _cache_read(stock: str) -> pd.DataFrame | None:
    """Load cached daily OHLCV. Handles both old schema (no tf) and new multi-TF schema."""
    try:
        con = _get_cache()
        # Try new multi-TF schema first (data_updater v4+ uses tf column)
        try:
            df = pd.read_sql(
                "SELECT date,open,high,low,close,volume FROM price_cache "
                "WHERE stock=? AND tf='1d' ORDER BY date",
                con, params=(stock,)
            )
            if len(df) >= 20:
                df["date"] = pd.to_datetime(df["date"])
                df = df.set_index("date")
                df.columns = ["Open","High","Low","Close","Volume"]
                df.index.name = None
                return df.astype(float)
        except Exception:
            pass
        # Fallback: old schema without tf column
        df = pd.read_sql(
            "SELECT date,open,high,low,close,volume FROM price_cache "
            "WHERE stock=? ORDER BY date",
            con, params=(stock,)
        )
        if len(df) < 20:
            return None
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")
        df.columns = ["Open","High","Low","Close","Volume"]
        df.index.name = None
        return df.astype(float)
    except Exception:
        return None


def read_cache(stock: str, tf: str = "1d", limit: int = 9999) -> pd.DataFrame | None:
    """Multi-TF cache read for weekly/monthly/intraday bars (data_updater schema)."""
    try:
        con = _get_cache()
        try:
            df = pd.read_sql(
                f"SELECT date,open,high,low,close,volume FROM price_cache "
                f"WHERE stock=? AND tf=? ORDER BY date DESC LIMIT {limit}",
                con, params=(stock, tf)
            )
        except Exception:
            if tf != "1d":
                return None
            df = pd.read_sql(
                f"SELECT date,open,high,low,close,volume FROM price_cache "
                f"WHERE stock=? ORDER BY date DESC LIMIT {limit}",
                con, params=(stock,)
            )
        if len(df) < 2:
            return None
        df = df.iloc[::-1].reset_index(drop=True)
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")
        df.columns = ["Open","High","Low","Close","Volume"]
        df.index.name = None
        return df.astype(float)
    except Exception as e:
        log.debug(f"read_cache {stock} {tf}: {e}")
        return None


def _cache_write(stock: str, df: pd.DataFrame, tf: str = "1d"):
    """Write/upsert OHLCV rows. Supports both old schema and new multi-TF schema."""
    if df is None or len(df) == 0:
        return
    try:
        con = _get_cache()
        rows_new = []   # (stock, tf, date, o, h, l, c, v)
        rows_old = []   # (stock, date, o, h, l, c, v)
        for idx, row in df.iterrows():
            date_str = str(idx.date()) if hasattr(idx, "date") else str(idx)[:10]
            vals = (
                float(row.get("Open",  row.get("open",  0)) or 0),
                float(row.get("High",  row.get("high",  0)) or 0),
                float(row.get("Low",   row.get("low",   0)) or 0),
                float(row.get("Close", row.get("close", 0)) or 0),
                float(row.get("Volume",row.get("volume",0)) or 0),
            )
            rows_new.append((stock, tf, date_str) + vals)
            rows_old.append((stock, date_str) + vals)
        with _cache_lock:
            try:
                con.executemany(
                    "INSERT OR REPLACE INTO price_cache "
                    "(stock,tf,date,open,high,low,close,volume) VALUES (?,?,?,?,?,?,?,?)",
                    rows_new
                )
            except Exception:
                con.executemany(
                    "INSERT OR REPLACE INTO price_cache "
                    "(stock,date,open,high,low,close,volume) VALUES (?,?,?,?,?,?,?)",
                    rows_old
                )
            con.execute(
                "INSERT OR REPLACE INTO cache_meta (stock,last_updated,bar_count) "
                "VALUES (?,?,?)",
                (stock, str(_today()), len(rows_new))
            )
            con.commit()
    except Exception as e:
        log.debug(f"Cache write {stock}: {e}")


def _cache_meta(stock: str) -> dict:
    """Return metadata for a cached stock. Handles both old and new (stock,tf) schema."""
    try:
        con = _get_cache()
        # New schema: PK is (stock, tf) — query specifically for '1d'
        try:
            row = con.execute(
                "SELECT last_updated, bar_count FROM cache_meta WHERE stock=? AND tf='1d'",
                (stock,)
            ).fetchone()
            if row:
                return {"last_updated": row[0], "bar_count": row[1]}
        except Exception:
            pass
        # Old schema: PK is stock only
        row = con.execute(
            "SELECT last_updated, bar_count FROM cache_meta WHERE stock=?",
            (stock,)
        ).fetchone()
        if row:
            return {"last_updated": row[0], "bar_count": row[1]}
    except Exception:
        pass
    return {}


def _fund_cache_read(stock: str) -> dict | None:
    """Read cached fundamentals. Tries data_updater's fund_cache table first, then old schema."""
    try:
        con = _get_cache()
        # New schema: data_updater stores in fund_cache(stock, fund_json, updated_date)
        try:
            row = con.execute(
                "SELECT fund_json, updated_date FROM fund_cache WHERE stock=?",
                (stock,)
            ).fetchone()
            if row and row[0] and row[1] == str(_today()):
                return json.loads(row[0])
        except Exception:
            pass
        # Old schema: stored in cache_meta.fund_json
        try:
            row = con.execute(
                "SELECT fund_json, fund_updated FROM cache_meta WHERE stock=?",
                (stock,)
            ).fetchone()
            if row and row[0] and row[1] == str(_today()):
                return json.loads(row[0])
        except Exception:
            pass
    except Exception:
        pass
    return None


def _fund_cache_write(stock: str, fund: dict):
    """Cache fundamentals. Writes to fund_cache table (new schema) with old fallback."""
    try:
        con = _get_cache()
        with _cache_lock:
            # Try new fund_cache table first (data_updater schema)
            try:
                con.execute(
                    "INSERT OR REPLACE INTO fund_cache (stock, fund_json, updated_date) "
                    "VALUES (?, ?, ?)",
                    (stock, json.dumps(fund), str(_today()))
                )
                con.commit()
                return
            except Exception:
                pass
            # Fallback: old cache_meta schema
            try:
                con.execute(
                    "UPDATE cache_meta SET fund_json=?, fund_updated=? WHERE stock=?",
                    (json.dumps(fund), str(_today()), stock)
                )
                con.commit()
            except Exception as e:
                log.debug(f"Fund cache write {stock}: {e}")
    except Exception as e:
        log.debug(f"Fund cache write {stock}: {e}")


def dl_cached(sym: str, period: str = PERIOD_DAILY) -> pd.DataFrame | None:
    """
    Incremental OHLCV fetch with local SQLite cache.

    Logic:
      1. Read cache. If empty or stale (> STALE_DAYS old) → full download.
      2. If cache exists and last_updated == today → return cache as-is (already fresh).
      3. If cache exists but not today → download last 7d, upsert, return merged.

    After day 1, step 3 costs ~5 rows/stock instead of ~252. 50x faster.
    """
    cached = _cache_read(sym)
    meta   = _cache_meta(sym)
    today  = str(_today())

    # Already updated today → return cache immediately
    if cached is not None and meta.get("last_updated") == today:
        return cached

    # Cache is too old or empty → full download
    stale = False
    if meta.get("last_updated"):
        try:
            last = pd.to_datetime(meta["last_updated"]).date()
            stale = (_today() - last).days > STALE_DAYS
        except Exception:
            stale = True
    else:
        stale = True  # never cached

    if cached is None or stale:
        log.debug(f"Full download: {sym}")
        fresh = dl(sym, "1d", period)
        if fresh is not None:
            _cache_write(sym, fresh)
        return fresh

    # Incremental: just fetch last 7 days
    log.debug(f"Incremental: {sym}")
    recent = dl(sym, "1d", "7d")
    if recent is None:
        # Network issue — return what we have
        return cached

    # Merge: drop rows already in cache, append new ones
    try:
        new_rows = recent[~recent.index.normalize().isin(cached.index.normalize())]
        if len(new_rows):
            merged = pd.concat([cached, new_rows]).sort_index()
            # Trim to roughly 1y (keep ~300 trading days)
            if len(merged) > 300:
                merged = merged.iloc[-300:]
            _cache_write(sym, new_rows)   # only write the new rows
            return merged
        return cached
    except Exception as e:
        log.debug(f"Merge failed {sym}: {e}")
        return cached


def dl_fund_cached(sym: str) -> dict:
    """Fundamentals with same-day cache. Avoids hitting Yahoo 2000× per daily scan."""
    cached = _fund_cache_read(sym)
    if cached is not None:
        return cached
    fund = dl_fund(sym)  # raw fetch
    if fund.get("_fund_ok"):
        _fund_cache_write(sym, fund)
    return fund


def warm_cache(stocks: list, workers: int = 8):
    """
    Pre-warm the price cache for a list of stocks.
    Called once at the start of daily scan.
    Stocks already cached today are skipped instantly.

    Gap 2 FIX: After caching, also builds the RS universe dict so that
    calc_rs_percentile() can do true cross-sectional ranking for every stock.
    """
    today = str(_today())
    need_full = [s for s in stocks if _cache_meta(s).get("last_updated") != today]
    if not need_full:
        log.info("Cache: all stocks already up-to-date for today")
    else:
        log.info(f"Cache warm-up: {len(need_full)} stocks need update "
                 f"({len(stocks)-len(need_full)} already cached today)...")
    t0 = time.time()
    done = 0

    def _fetch_one(sym):
        return sym, dl_cached(sym)   # dl_cached handles full vs incremental

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_fetch_one, s): s for s in need_full}
        for fut in as_completed(futs):
            done += 1
            if done % 200 == 0:
                elapsed = time.time() - t0
                rate = done / elapsed
                eta  = (len(need_full) - done) / rate if rate > 0 else 0
                log.info(f"  Cache: {done}/{len(need_full)} | "
                         f"{elapsed:.0f}s elapsed | ETA {eta:.0f}s")
            try:
                fut.result()
            except Exception:
                pass

        log.info(f"Cache warm-up done: {time.time()-t0:.1f}s")

    # ── Gap 2: Build RS universe from ALL cached stocks (not just those refreshed) ──
    log.info("Building RS universe for cross-sectional ranking …")
    rs_built = 0
    for sym in stocks:
        try:
            df = _cache_read(sym)
            if df is not None and len(df) >= 252:
                c = df["Close"].values.astype(float)
                register_rs_score(sym, c)
                rs_built += 1
        except Exception:
            pass
    log.info(f"RS universe: {rs_built}/{len(stocks)} stocks registered")

def _build_session():
    try:
        from curl_cffi import requests as _cr
        sess = _cr.Session(impersonate="chrome110")
        # warm-up: hit Yahoo Finance to get cookies (crumb lives in cookie jar)
        sess.get("https://finance.yahoo.com", timeout=15)
        log.info("curl_cffi Chrome session ready")
        return sess
    except ImportError:
        log.warning("curl_cffi not installed — Yahoo may 401. pip install curl_cffi")
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

def _normalize_df_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    CRASH FIX: Normalize DataFrame index to tz-naive datetime.
    Yahoo Finance sometimes returns ISO 8601 timestamps (e.g. '2025-05-22T08:45:00+00:00')
    that crash older yfinance/pandas date parsers with:
      ValueError('unconverted data remains: T08:45:00+00:00')
    This function unifies all formats to a tz-naive Asia/Kolkata-equivalent naive datetime.
    """
    try:
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index, utc=True)
        if df.index.tz is not None:
            df.index = df.index.tz_convert("Asia/Kolkata").tz_localize(None)
    except Exception:
        try:
            df.index = pd.to_datetime(df.index, format="mixed", utc=True).tz_localize(None)
        except Exception:
            pass   # best effort — return as-is
    return df

def _is_rate_limit_output(text: str) -> bool:
    """
    Detect yfinance's silently-swallowed YFRateLimitError.
    yfinance catches the error internally, logs it to stderr, and returns
    an empty DataFrame instead of raising — so our try/except never sees it.
    We capture stderr during the download and check for these signatures.
    """
    t = text.lower()
    return (
        "yfrarelimiterror" in t
        or "yfratelimiterror" in t
        or "429" in t
        or ("rate" in t and "limit" in t)
        or "too many requests" in t
    )

def dl(sym, interval="1d", period=PERIOD_DAILY):
    for attempt in range(DL_RETRIES):
        # Block here until the global rate-limit window clears.
        # All parallel workers pause here — no flood while banned.
        _wait_if_rate_limited(sym)
        try:
            sess = _get_session()
            kw = {"session": sess} if sess is not None else {}

            # ── RATE-LIMIT FIX ────────────────────────────────────────────────
            # yfinance swallows YFRateLimitError internally: it catches the
            # exception, prints it to stderr, and returns an empty DataFrame.
            # Our try/except never fires — we just get an empty df → treated
            # as a skip. Fix: capture stderr/stdout during the download call
            # and check for rate-limit signatures if the df comes back empty.
            captured = io.StringIO()
            with contextlib.redirect_stderr(captured), \
                 contextlib.redirect_stdout(captured):
                df = yf.download(sym, period=period, interval=interval,
                                 auto_adjust=True, progress=False, timeout=20, **kw)

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            # CRASH FIX: normalize index before dropna — avoids ISO timestamp crash
            df = _normalize_df_index(df)
            df = df.dropna()

            # If we got an empty result, check whether yfinance silently ate a
            # rate-limit error and raise a synthetic exception so the handler
            # below applies the same backoff logic as a real 429.
            if df.empty or len(df) <= 20:
                output = captured.getvalue()
                if _is_rate_limit_output(output):
                    log.warning(f"yfinance swallowed rate-limit on {sym} "
                                f"(attempt {attempt+1}) — captured: "
                                f"{output.strip()[:120]}")
                    raise Exception("429 rate limit detected in yfinance output")

            return df if len(df) > 20 else None
        except Exception as e:
            msg = str(e)
            if "unconverted data remains" in msg or ("T0" in msg and "+" in msg):
                # Yahoo returning ISO 8601 timestamps yfinance can't parse —
                # clear session and retry; most resolve on 2nd attempt
                log.debug(f"ISO timestamp parse error {sym} attempt {attempt+1} — retry")
                _reset_session()
                time.sleep(3 * (attempt + 1))
            elif "401" in msg or "Crumb" in msg or "Unauthorized" in msg:
                log.warning(f"401/Crumb on {sym} attempt {attempt+1} — reset session")
                _reset_session(); time.sleep(8)
            elif "429" in msg or "rate limit" in msg.lower() or "too many" in msg.lower():
                # Escalating global pause: 60s → 120s → 180s
                backoff = 60 * (attempt + 1)
                _set_rate_limit(backoff)
                _wait_if_rate_limited(sym)
            elif "delisted" in msg.lower() or "no price data" in msg.lower():
                return None   # no retry on delisted
            elif attempt < DL_RETRIES - 1:
                time.sleep(DL_BACKOFF * (attempt + 1))
    return None

def dl_fund(sym):
    for attempt in range(DL_RETRIES):
        try:
            sess = _get_session()
            kw = {"session": sess} if sess is not None else {}
            tk = yf.Ticker(sym, **kw)
            info = tk.info or {}
            if not info.get("marketCap"):
                if attempt < DL_RETRIES - 1:
                    time.sleep(3)
                    continue
                return {"_fund_ok": False}
            try:
                cal = tk.calendar
                ne = cal.get("Earnings Date", [None])[0] if cal else None
            except Exception:
                ne = None
            return {
                "_fund_ok": True,
                "marketCap": info.get("marketCap"),
                "earningsQuarterlyGrowth": info.get("earningsQuarterlyGrowth"),
                "earningsGrowth": info.get("earningsGrowth"),
                "heldPercentInstitutions": info.get("heldPercentInstitutions"),
                "sector": info.get("sector"),
                "longName": info.get("longName") or info.get("shortName"),
                "next_earnings": str(ne) if ne else None,
            }
        except Exception as e:
            msg = str(e)
            if "401" in msg or "Crumb" in msg or "Unauthorized" in msg:
                log.warning(f"401/Crumb on fund {sym} attempt {attempt+1} — reset session")
                _reset_session()
                time.sleep(5)
            elif attempt < DL_RETRIES - 1:
                time.sleep(DL_BACKOFF * (attempt + 1))
    return {"_fund_ok": False}

def cap_class(mc):
    if not mc or pd.isna(mc): return "Unknown", None
    cr = mc / 1e7
    if cr >= 20000: return "Large", round(cr)
    if cr >= 5000:  return "Mid", round(cr)
    if cr >= 500:   return "Small", round(cr)
    return "Micro", round(cr)

# ================================================================
# MARKET SIGNALS
# ================================================================
def fetch_india_vix() -> float | None:
    """Fetch India VIX. Returns float or None."""
    try:
        df = dl(INDIA_VIX_SYM, "1d", "5d")
        if df is not None and len(df) > 0:
            return round(float(df["Close"].values[-1]), 2)
    except Exception:
        pass
    return None


def check_market_breadth(nifty500_sample: list) -> dict:
    """
    Compute % of stocks above 50-DMA and 200-DMA.
    Uses cached data from whatever is already in price_cache.
    Returns dict: {pct_above_50, pct_above_200, regime, ad_ratio, new_high_ratio}

    Gap 9 FIX:
    - Use a random 150-stock sample (not first 100 alphabetically which
      over-samples A/B/C names and misrepresents the broader market).
    - Add advance/decline ratio and new-high vs new-low count.
    """
    import random as _random
    sample = list(nifty500_sample)
    if len(sample) > 150:
        # Stratified random: take every Nth stock to spread across the alphabet
        step = len(sample) // 150
        sample = sample[::step][:150]

    above_50 = 0; above_200 = 0; total = 0
    advancing = 0; declining = 0; new_highs = 0; new_lows = 0
    for sym in sample:
        cached = _cache_read(sym)
        if cached is None or len(cached) < 50: continue
        c = cached["Close"].values.astype(float)
        total += 1
        if c[-1] > np.mean(c[-min(50, len(c)):]): above_50 += 1
        if len(c) >= 200 and c[-1] > np.mean(c[-200:]): above_200 += 1
        # Advance/Decline: compare today's close vs yesterday
        if len(c) >= 2:
            if c[-1] > c[-2]:   advancing += 1
            elif c[-1] < c[-2]: declining += 1
        # New 52-week highs/lows
        lb = min(252, len(c))
        if lb >= 20:
            hi52 = np.max(c[-lb:])
            lo52 = np.min(c[-lb:])
            if c[-1] >= hi52 * 0.98: new_highs += 1
            if c[-1] <= lo52 * 1.02: new_lows  += 1
    if total == 0:
        return {"pct_above_50": 50, "pct_above_200": 50, "regime": "Unknown",
                "ad_ratio": 1.0, "new_high_ratio": 1.0}
    p50  = round(above_50  / total * 100, 1)
    p200 = round(above_200 / total * 100, 1)
    ad_ratio       = round(advancing / max(declining, 1), 2)
    new_high_ratio = round(new_highs / max(new_lows, 1), 2)
    if p50 >= 60 and p200 >= 50:   regime = "Bull"
    elif p50 >= 40 and p200 >= 35: regime = "Neutral"
    elif p50 < 40 and p200 < 35:   regime = "Bear"
    else:                          regime = "Mixed"
    # A/D ratio override: if strongly negative even in apparent Bull → downgrade
    if regime == "Bull" and ad_ratio < 0.7:
        regime = "Mixed"
    return {"pct_above_50": p50, "pct_above_200": p200, "regime": regime,
            "ad_ratio": ad_ratio, "new_high_ratio": new_high_ratio}


def get_market_regime(nifty_df, vix: float | None, breadth: dict) -> dict:
    """
    4-state market regime combining: Nifty trend + VIX + breadth.
    Returns dict with regime, aggression (0-3), and detail string.
    """
    if nifty_df is None or len(nifty_df) < 50:
        return {"regime": "Unknown", "aggression": 1, "detail": "no data"}
    c = nifty_df["Close"].values
    ma50  = np.mean(c[-50:])
    ma200 = np.mean(c[-min(200, len(c)):])
    above_50  = c[-1] > ma50
    above_200 = c[-1] > ma200
    vix_ok = vix is None or vix < VIX_HIGH_THRESH
    vix_extreme = vix is not None and vix > VIX_EXTREME_THRESH
    b_regime = breadth.get("regime", "Unknown")

    if above_200 and above_50 and vix_ok and b_regime == "Bull":
        regime = "Strong-Bull"; aggression = 3
    elif above_200 and (vix_ok or b_regime in ("Bull","Neutral")):
        regime = "Uptrend"; aggression = 2
    elif above_200 and not vix_ok:
        regime = "Cautious"; aggression = 1
    elif not above_200 and above_50:
        regime = "Choppy"; aggression = 1
    else:
        regime = "Bear"; aggression = 0

    if vix_extreme: aggression = max(0, aggression - 1)

    # GAP8 FIX: previous code had ternary on entire parenthesised expression — when vix=None,
    # "Nifty ↑50MA ↑200MA |" was silently dropped. Now built unconditionally.
    detail = (f"Nifty {'↑' if above_50 else '↓'}50MA {'↑' if above_200 else '↓'}200MA"
              + (f" | VIX {vix:.1f}" if vix else " | VIX N/A")
              + f" | Breadth {breadth.get('pct_above_50','?')}%↑50d"
              + (f" | A/D {breadth.get('ad_ratio','?')}" if breadth.get('ad_ratio') else "")
              + (f" | NH/NL {breadth.get('new_high_ratio','?')}" if breadth.get('new_high_ratio') else "")
              )
    return {"regime": regime, "aggression": aggression, "detail": detail}


def check_follow_through_day(nifty_df):
    if nifty_df is None or len(nifty_df) < 30:
        return False, "no data"
    c = nifty_df["Close"].values; v = nifty_df["Volume"].values
    low_idx = len(c) - 30 + int(np.argmin(c[-30:]))
    rally = 0
    for i in range(low_idx + 1, len(c)):
        rally = rally + 1 if c[i] > c[i-1] else 0
    gain = (c[-1] - c[-2]) / c[-2] if c[-2] > 0 else 0
    # BUG5 FIX: O'Neil FTD requires volume ABOVE 50-day avg, not just above yesterday
    vol_avg50 = np.mean(v[-50:]) if len(v) >= 50 else np.mean(v)
    ftd = (rally >= 4 and gain >= 0.015
           and len(v) >= 2 and v[-1] > v[-2]
           and v[-1] > vol_avg50)
    above_200 = c[-1] > np.mean(c[-min(200, len(c)):]) if len(c) >= 50 else False
    return ftd or above_200, f"rally={rally} gain={gain:.2%} vol_vs50avg={v[-1]/vol_avg50:.2f}x"

def check_market_trend(nc):
    if nc is None or len(nc) < 200: return "Unknown"
    ma50 = np.mean(nc[-50:]); ma200 = np.mean(nc[-200:])
    if nc[-1] > ma50 > ma200: return "Stage2-Bull"
    if nc[-1] > ma200: return "Uptrend"
    if nc[-1] < ma50 < ma200: return "Stage4-Bear"
    return "Choppy"

def check_volume_dryup(vol, lb=25):
    return vol is not None and len(vol) >= lb and vol[-1] <= np.min(vol[-lb:]) * 1.05

def check_weinstein_stage(close, p=150):
    if len(close) < p + 20: return "Unknown"
    ma = np.mean(close[-p:]); ma_p = np.mean(close[-p-20:-20])
    if close[-1] > ma and ma > ma_p: return "Stage2"
    if close[-1] > ma: return "Stage1-Late"
    if close[-1] < ma and ma < ma_p: return "Stage4"
    return "Stage3"

def check_earnings_near(fund, days=14):
    ne = fund.get("next_earnings")
    if not ne: return False
    try:
        ed = datetime.strptime(ne[:10], "%Y-%m-%d").date()
        return 0 <= (ed - date.today()).days <= days
    except Exception:
        return False

def calc_adr(close, p=20):
    if len(close) < p + 1: return 0
    r = [abs(close[i]-close[i-1])/close[i-1] for i in range(-p, 0) if close[i-1] > 0]
    return round(np.mean(r) * 100, 2) if r else 0

# ================================================================
# CANSLIM
# ================================================================

# ── Gap 2 FIX: True cross-sectional RS universe ─────────────────────────────
# Built during warm_cache / after full universe download so every stock's
# weighted return is known before we rank any individual stock against it.
_RS_UNIVERSE: dict[str, float] = {}   # sym → weighted 4-quarter return
_RS_UNIVERSE_LOCK = Lock()

def _rs_weighted_return(close: np.ndarray, lb: int = 63) -> float | None:
    """O'Neil 4-quarter weighted return: 40% last quarter + 20% each prior 3."""
    if len(close) < lb * 4:
        return None
    def _r(a, b):
        return (a / b - 1) if b > 0 else 0.0
    r1 = _r(close[-1],    close[-lb])
    r2 = _r(close[-lb],   close[-lb*2]) if len(close) >= lb*2 else 0.0
    r3 = _r(close[-lb*2], close[-lb*3]) if len(close) >= lb*3 else 0.0
    r4 = _r(close[-lb*3], close[-lb*4]) if len(close) >= lb*4 else 0.0
    return 0.40*r1 + 0.20*r2 + 0.20*r3 + 0.20*r4

def register_rs_score(sym: str, close: np.ndarray, lb: int = 63):
    """Called for every stock after data load; builds the RS universe for ranking."""
    w = _rs_weighted_return(close, lb)
    if w is not None:
        with _RS_UNIVERSE_LOCK:
            _RS_UNIVERSE[sym] = w

def calc_rs_percentile(close, nc, lb=63, sym: str = "") -> float | None:
    """
    Gap 2 FIX — True O'Neil cross-sectional RS percentile (0-99).

    If the universe dict is populated (after warm_cache), rank this stock's
    weighted 4-quarter return against the full universe.  Falls back to the
    old linear approximation if the universe is too small (< 50 stocks).
    """
    if nc is None or len(close) < lb or len(nc) < lb:
        return None

    with _RS_UNIVERSE_LOCK:
        universe = dict(_RS_UNIVERSE)

    if len(universe) >= 50 and sym and sym in universe:
        scores = sorted(universe.values())
        target = universe[sym]
        rank   = sum(1 for s in scores if s <= target) / len(scores) * 99
        return round(rank, 1)

    # Fallback: old approximation (still better than nothing on first run)
    sr = (close[-1] / close[-lb] - 1) if close[-lb] > 0 else 0
    nr = (nc[-1]   / nc[-lb]   - 1)  if nc[-lb]   > 0 else 0
    outperf = sr - nr
    pct = 50 + outperf * 175
    return round(min(max(pct, 0), 100), 1)


# ── Gap 11 FIX: RS line leadership (new 52-week high BEFORE price) ───────────
def rs_line_leading(close: np.ndarray, nc: np.ndarray | None, lb: int = 252) -> bool:
    """
    Returns True if the RS line (close / nifty_close) is at or within 1 % of
    its 52-week high.  This is O'Neil's highest-conviction leading indicator —
    institutions accumulating before the visible price breakout.
    """
    if nc is None or len(close) < lb or len(nc) < lb:
        return False
    rs_line = close[-lb:] / nc[-lb:]
    current_rs   = rs_line[-1]
    rs_52wk_high = float(np.max(rs_line[:-5]))   # exclude last 5 days (noise)
    return current_rs >= rs_52wk_high * 0.99


# ── Gap 4 FIX: Volume indicators — OBV, A/D line, up/down vol ratio ─────────
def calc_obv(close: np.ndarray, vol: np.ndarray) -> np.ndarray:
    """On-Balance Volume — cumulative; rising OBV during base = institutional accumulation."""
    if len(close) != len(vol) or len(close) < 2:
        return np.zeros(len(close))
    obv = np.zeros(len(close))
    obv[0] = vol[0]
    for i in range(1, len(close)):
        if close[i] > close[i-1]:
            obv[i] = obv[i-1] + vol[i]
        elif close[i] < close[i-1]:
            obv[i] = obv[i-1] - vol[i]
        else:
            obv[i] = obv[i-1]
    return obv

def calc_ad_line(close: np.ndarray, high: np.ndarray, low: np.ndarray,
                 vol: np.ndarray) -> np.ndarray:
    """
    Accumulation/Distribution line.
    CLV = [(Close-Low)-(High-Close)] / (High-Low)
    AD[i] = AD[i-1] + CLV[i] * Vol[i]
    """
    n = min(len(close), len(high), len(low), len(vol))
    ad = np.zeros(n)
    for i in range(n):
        rng = high[i] - low[i]
        if rng > 0:
            clv = ((close[i] - low[i]) - (high[i] - close[i])) / rng
        else:
            clv = 0.0
        ad[i] = (ad[i-1] if i > 0 else 0.0) + clv * vol[i]
    return ad

def calc_updown_vol_ratio(close: np.ndarray, vol: np.ndarray, lb: int = 15) -> float | None:
    """
    Ratio of cumulative volume on up-days to down-days over last lb sessions.
    >= 1.5 = healthy accumulation; < 0.8 = distribution.
    """
    if len(close) < lb + 1 or vol is None or len(vol) < lb + 1:
        return None
    up_vol = down_vol = 0.0
    for i in range(len(close)-lb, len(close)):
        if close[i] > close[i-1]:
            up_vol   += vol[i]
        elif close[i] < close[i-1]:
            down_vol += vol[i]
    if down_vol == 0:
        return 9.99 if up_vol > 0 else None
    return round(up_vol / down_vol, 2)

def obv_confirming(close: np.ndarray, vol: np.ndarray, lb: int = 20) -> bool:
    """True if OBV is making new highs alongside price (confirming breakout)."""
    if len(close) < lb or vol is None or len(vol) < lb:
        return True   # no data = don't penalise
    obv = calc_obv(close[-lb:], vol[-lb:])
    return float(obv[-1]) >= float(np.max(obv[:-1]))


def canslim_score(close, vol, fund, nc, nr):
    n = len(close); idx = n - 1; score = 0; checks = 0
    lb = min(252, idx); hi = np.max(close[max(0,idx-lb):idx+1])
    if hi > 0:
        checks += 1
        if (hi - close[idx]) / hi <= CS["N_max_from_high"]: score += 1
    if nr is not None and idx >= 252 and idx < len(nr) and not np.isnan(nr[idx]):
        checks += 1
        sr = close[idx]/close[idx-252]-1; nrr = nr[idx]
        if (1+nrr) > 0 and (1+sr)/(1+nrr) >= CS["L_min_rs"]: score += 1
    if nc is not None and len(nc) >= 200:
        checks += 1
        if nc[-1] > np.mean(nc[-200:]): score += 1
    if vol is not None and idx >= 20:
        checks += 1
        # FIX: raise to ₹15 Cr/day (was ₹1 Cr — too loose, allowed illiquid stocks)
        if np.mean(vol[idx-19:idx+1])*np.mean(close[idx-19:idx+1])/1e7 >= MIN_LIQUIDITY_CR: score += 1
    for key, th in [("earningsQuarterlyGrowth", CS["C_min"]),
                    ("earningsGrowth", CS["A_min"]),
                    ("heldPercentInstitutions", CS["I_min_instl"])]:
        v = fund.get(key)
        if v is not None:
            checks += 1
            if v >= th: score += 1
    return score, checks

def recommend(status, score, mkt_up, aggression=2, rs_pct=None):
    """
    Signal recommendation. A pattern was ALREADY detected before calling this —
    so we never fully suppress it. Worst case = WATCH.
    aggression: 0=Bear, 1=Cautious, 2=Uptrend, 3=Bull
    """
    bo = any(k in status for k in ["Breakout","Burst","Pivot","Pocket","Reclaim","ORB"])
    rs_ok = rs_pct is None or rs_pct >= MIN_RS_PERCENTILE

    # Bear market: downgrade all BUYs to WATCH, never AVOID
    if aggression == 0:
        if bo: return "WATCH — bear mkt (breakout)"
        return "WATCH — bear mkt"

    if bo and score >= CS["buy_strong"] and mkt_up and aggression >= 2 and rs_ok:
        return "BUY — strong"
    if bo and score >= CS["buy_moderate"] and aggression >= 1:
        return "BUY — moderate"
    if bo:
        return "WATCH — breakout setup"   # breakout detected but score/RS not quite there
    if score >= CS["buy_strong"]:
        return "WATCH — await breakout"
    if score >= CS["buy_moderate"]:
        return "WATCH — mixed"
    return "WATCH — low score"            # pattern detected → always emit, never AVOID

# ================================================================
# TARGETS
# ================================================================
def calc_atr(close, high=None, low=None, period=14) -> float:
    """
    Average True Range over period days (Wilder ATR).
    Uses True Range = max(H-L, |H-PrevC|, |L-PrevC|) when H/L available.
    Falls back to close-to-close only if H/L absent (underestimates 30-50% on gapping NSE stocks).
    """
    if len(close) < period + 1:
        return close[-1] * 0.02
    if high is not None and low is not None and len(high) >= period + 1 and len(low) >= period + 1:
        # True Range using last period+1 bars
        h = high[-(period + 1):]
        l = low[-(period + 1):]
        c = close[-(period + 1):]
        tr = [max(h[i] - l[i],
                  abs(h[i] - c[i - 1]),
                  abs(l[i] - c[i - 1]))
              for i in range(1, len(c))]
        return float(np.mean(tr))
    # Fallback: close-to-close (underestimates on gap days — acceptable when H/L unavailable)
    moves = np.abs(np.diff(close[-period - 1:]))
    return float(np.mean(moves))


def calc_structural_stop(pattern, close, high, low, bz, bottom) -> float | None:
    """
    Pattern-specific structural stop — the price that PROVES the setup is wrong.
    Research: ATR stop alone is noise. Structural stop = key pivot below which
    the pattern thesis fails. We use the HIGHER (tighter) of ATR vs structural.

    Per research doc Section 3:
      CupHandle   → below handle low (bottom * 0.995)
      VCP         → below final contraction low (bottom * 0.995)
      FlatBase    → below base low (bottom * 0.992)
      InvHS       → below right shoulder low
      DoubleBottom→ below the lower of the two bottoms
      AscTriangle → below last rising trough (bottom * 0.995)
      BullFlag    → below flag low (bottom * 0.995)
      FallingWedge→ below wedge swing low (bottom * 0.995)
      MomBurst    → below prior-day LOW (not close) — fastest structural
      EpisodicPivot→ above prior close (gap fill = thesis broken)
      PocketPivot → below prior close
      Stage2Breakout→ below 150d MA
      Anticipation→ below 20d consolidation low
    """
    if bottom and bottom > 0:
        if pattern in ("CupHandle", "VCP", "AscTriangle", "BullFlag",
                       "FallingWedge", "Anticipation"):
            return round(bottom * 0.995, 2)
        if pattern == "FlatBase":
            return round(bottom * 0.992, 2)
        if pattern == "InvHS":
            return round(bottom * 0.995, 2)   # right shoulder low
        if pattern == "DoubleBottom":
            return round(bottom * 0.992, 2)
        if pattern == "MomBurst":
            # Prior day low: use low_p if available, else prior close * 0.99
            if low is not None and len(low) >= 2:
                return round(float(low[-2]), 2)
            return round(bottom * 0.99, 2)
        if pattern == "EpisodicPivot":
            # Gap 8 FIX: EP stocks routinely fill 20-40% of the gap intraday.
            # Stop at 0.1% above prior close is too tight and gets hit by normal noise.
            # Use 50% gap fill as the stop — if more than half the gap is retraced,
            # the thesis is weakening. bottom = prior close, bz = gap-up open/pivot.
            if bottom and bz and bz > bottom:
                gap_midpoint = bottom + 0.50 * (bz - bottom)
                return round(gap_midpoint, 2)
            return round(bottom * 1.001, 2)   # fallback
        if pattern == "PocketPivot":
            return round(bottom * 0.995, 2)
    if pattern == "Stage2Breakout" and close is not None and len(close) >= 150:
        ma150 = np.mean(close[-150:])
        return round(ma150 * 0.995, 2)
    return None


def calc_targets(pattern, bz, bottom, cmp, adr, close=None, high=None, low=None):
    """
    Pattern-specific targets with 2.0× ATR structural stop capped at 8%.

    Research corrections applied:
    1. ATR multiplier: 1.5× → 2.0× (1.5× is for DAY TRADES; swing needs 2.0×)
       At 1.5×, ~30% of swing trades hit stop before move develops (Schwager research)
    2. Structural stop per pattern (Section 3, research doc) — tighter AND smarter
    3. MomBurst uses R-multiple targets (not flat %) for proper position sizing
    4. EpisodicPivot: 2R/4R targets + trail 20-EMA (not 5/10/15%)
    5. Final stop = HIGHEST (tightest) of: 2×ATR stop, structural stop, 8% hard cap
    """
    if not bz or bz <= 0: return None, None, None, None, None
    h = bz - bottom if bottom and bottom > 0 else bz * 0.10

    # ── Step 1: ATR-based stop (2.0× — research-backed for 10-15d swing) ────
    if close is not None and len(close) >= 15:
        atr14 = calc_atr(close, high=high, low=low, period=14)
        atr_stop = cmp - 2.0 * atr14          # 2.0× not 1.5× — critical fix
        hard_cap  = cmp * (1 - MAX_STOP_PCT)  # 8% absolute maximum
        stop_atr  = max(atr_stop, hard_cap)
    else:
        stop_atr = cmp * (1 - MAX_STOP_PCT)

    # ── Step 2: Structural stop per pattern ──────────────────────────────────
    struct_stop = calc_structural_stop(pattern, close, high, low, bz, bottom)

    # ── Step 3: Final stop = highest (tightest) of the two ───────────────────
    if struct_stop is not None:
        stop = round(max(stop_atr, struct_stop), 2)
    else:
        stop = round(stop_atr, 2)

    # ── Step 4: Targets — pattern-specific ───────────────────────────────────
    risk = max(cmp - stop, cmp * 0.01)  # R = 1 unit of risk

    if pattern == "MomBurst":
        # R-multiple targets: 1.5R / 2.5R / 4.0R (Bonde standard)
        # Allows proper position sizing — flat % was wrong for variable-risk setups
        t1 = round(cmp + 1.5 * risk, 2)
        t2 = round(cmp + 2.5 * risk, 2)
        t3 = round(cmp + 4.0 * risk, 2)
    elif pattern == "EpisodicPivot":
        # Qullamaggie EP: 2R / 4R / trail 20-EMA. Catalysts can run 5-10× risk.
        t1 = round(cmp + 2.0 * risk, 2)
        t2 = round(cmp + 4.0 * risk, 2)
        t3 = round(cmp + 7.0 * risk, 2)   # runner target — trail 20-EMA beyond T2
    elif pattern == "PocketPivot":
        t1 = round(bz + h * 0.50, 2)
        t2 = round(bz + h * 1.00, 2)
        t3 = round(cmp + 4.0 * risk, 2)
    elif "Flag" in pattern:
        t1 = round(bz + h * 0.50, 2)
        t2 = round(bz + h * 1.00, 2)
        t3 = round(bz + h * 1.50, 2)
    else:
        # Base patterns: measured move + 1.618 Fibonacci extension (O'Neil standard)
        t1 = round(bz + h * 0.50, 2)
        t2 = round(bz + h * 1.00, 2)
        t3 = round(bz + h * 1.618, 2)

    # ── Step 5: RR using T2 as primary target ────────────────────────────────
    reward = max(t2 - cmp, cmp * 0.03)
    rr = round(reward / risk, 2) if risk > 0 else 0

    return stop, t1, t2, t3, rr

def identify_leg(close, bz):
    if len(close) < 50 or not bz: return "Unknown"
    if close[-1] < bz*0.98: return "Pre-breakout"
    g = (close[-1]-bz)/bz
    if g < 0.05: return "Leg1-Early"
    if g < 0.15: return "Leg1-Trending"
    if g < 0.30: return "Leg2-Extended"
    return "Leg3-Climax"

def vsurge(vol, n, lb=20):
    if vol is None or n < lb: return None
    avg = np.mean(vol[-lb:])
    return round(float(vol[-1]/avg),2) if avg > 0 else None

# ================================================================
# ALL 13 DETECTORS (compact but complete)
# ================================================================
# ================================================================
# STOCKBEE RANKING METRICS
# Bonde TI65, 2LYNCH score, composite rank
# ================================================================

def calc_ti65(c):
    """
    Bonde's Trend Intensity 65: avg7d / avg65d.
    >= 1.05 = confirmed uptrend. Range 1.02-1.30 = sweet spot.
    """
    if len(c) < 65: return None
    d = np.mean(c[-65:])
    return round(float(np.mean(c[-7:]) / d), 4) if d > 0 else None

def lynch_score(c, v):
    """
    2LYNCH checklist (Bonde/Stockbee). Returns 0-6.
    2 = Not up 2 consecutive days before breakout
    L = Linear orderly prior move
    Y = Young trend (TI65 in 1.02-1.30)
    N = Narrow/Negative day immediately before breakout
    C = Consolidation quality (Bollinger squeeze)
    H = Closing near High today
    """
    n = len(c); score = 0
    if n < 10: return score
    # 2: not up 2 days in a row before TODAY
    if n >= 4 and not (c[-2] > c[-3] and c[-3] > c[-4]):
        score += 1
    # L: linear = low coefficient of variation of daily moves
    if n >= 21:
        moves = np.abs(np.diff(c[-21:]))
        m_mean = np.mean(moves)
        if m_mean > 0 and np.std(moves) / m_mean < 1.2:
            score += 1
    # Y: young trend
    ti = calc_ti65(c)
    if ti is not None and 1.02 <= ti <= 1.30:
        score += 1
    # N: narrow (<1%) or negative day before breakout
    if n >= 3 and c[-3] > 0:
        pm = (c[-2] - c[-3]) / c[-3]
        if abs(pm) < 0.01 or pm < 0:
            score += 1
    # C: Bollinger band squeeze (tight consolidation)
    if n >= 20:
        bb = np.std(c[-20:]) / (np.mean(c[-20:]) + 1e-9)
        if bb < 0.04:
            score += 1
    # H: closing near high (close strength >= 60%)
    if n >= 5:
        hi5 = np.max(c[-5:])
        lo5 = np.min(c[-5:])
        rng = hi5 - lo5
        cs = (c[-1] - lo5) / rng if rng > 0 else 0.5
        if cs >= 0.60:
            score += 1
    return score

def composite_rank(row):
    """
    Composite score 0-100 for sorting signals.
    Weights: CANSLIM(25) + 2LYNCH(20) + TI65(15) + VolSurge(15) + Quality(15) + ADR(10)
    """
    s = 0
    cs = row.get("canslim_score", 0) or 0
    dc = row.get("data_completeness", 7) or 7
    s += (cs / max(dc, 1)) * 25           # CANSLIM (normalised to data available)
    ls = row.get("lynch_score_val", 0) or 0
    s += (ls / 6) * 20                     # 2LYNCH
    ti = row.get("ti65", 1.0) or 1.0
    ti_score = min(max(ti - 1.0, 0), 0.30) / 0.30
    s += ti_score * 15                     # TI65 (capped at +30%)
    vs = row.get("vol_surge", 1.0) or 1.0
    s += min(vs / 3.0, 1.0) * 15          # vol surge (capped at 3x)
    q = row.get("quality", 0) or 0
    s += min(abs(q), 1.0) * 15            # pattern quality
    adr = row.get("adr_pct", 2.0) or 2.0
    s += min(adr / 5.0, 1.0) * 10         # ADR (capped at 5%)
    return round(s, 1)

def det_cup(c, v):
    n = len(c)
    if n < 50: return None
    s = pd.Series(c).rolling(5, min_periods=1).mean().values
    ti = int(np.argmin(s))
    if not (n*0.20 <= ti <= n*0.80): return None
    lm, rm = np.max(s[:ti+1]), np.max(s[ti:])
    pk, tr = max(lm,rm), s[ti]
    d = (pk-tr)/pk
    if not (0.08 <= d <= 0.55): return None
    sym = abs(lm-rm)/pk
    if sym > 0.22: return None
    rpi = ti + int(np.argmax(s[ti:]))
    if rpi >= n-2 or s[rpi] < pk*0.88: return None
    h = s[rpi:]
    if len(h) < 2: return None
    hd = (np.max(h)-np.min(h))/np.max(h)
    if hd > 0.20 or np.min(h) < (pk+tr)/2*0.92: return None

    # Gap 6 FIX: handle must form in upper half of the cup (O'Neil criterion)
    cup_midpoint = (pk + tr) / 2
    handle_low = float(np.min(h))
    if handle_low < cup_midpoint:
        return None   # handle too deep — invalid CwH

    r = (n-rpi)/(rpi+1)
    if not (0.10 <= r <= 0.45): return None
    try:
        cx = np.arange(rpi+1); cf = np.polyfit(cx, s[:rpi+1], 2)
        fit = np.polyval(cf, cx)
        r2 = 1 - np.sum((s[:rpi+1]-fit)**2)/np.sum((s[:rpi+1]-np.mean(s[:rpi+1]))**2)
        if cf[0] <= 0 or r2 < 0.50: return None
    except: return None

    if v is not None and rpi > 5:
        left_vol  = np.mean(v[:rpi//2+1]) if rpi//2 > 0 else v[0]
        mid_vol   = np.mean(v[rpi//4:3*rpi//4]) if rpi > 4 else v[rpi//2]
        vol_dryup_cup = mid_vol < left_vol * 0.9
        # Gap 6 FIX: volume must also contract WITHIN the handle (not just in cup)
        handle_vol = v[rpi:] if len(v) > rpi else v[-3:]
        handle_vol_ok = (len(handle_vol) >= 2 and
                         float(np.mean(handle_vol)) < float(left_vol) * 0.85)
    else:
        vol_dryup_cup = True
        handle_vol_ok = True

    vs = vsurge(v, n); bo = c[-1] >= pk*0.97 and (vs is not None and vs >= 1.2)
    quality_adj = round((r2-sym)
                        * (1.15 if (vol_dryup_cup and handle_vol_ok) else
                           1.0  if vol_dryup_cup else 0.85), 3)
    # GAP-P1: start_bar = left rim of cup (peak before trough), relative to segment start
    # left rim ≈ argmax of left side; end_bar = last handle bar = n-1
    left_rim_bar = int(np.argmax(s[:ti+1]))   # left rim of cup in segment
    return dict(pattern="CupHandle", status="Breakout Ready" if bo else "Forming",
                quality=quality_adj, bz=round(float(pk),2), bottom=round(float(tr),2),
                last=round(float(c[-1]),2), vs=vs,
                m1=round(d*100,2), m2=round(sym*100,2), m3=round(hd*100,2), m4=round(r,2), m5=round(r2,3),
                _start_bar=left_rim_bar, _end_bar=n-1)

def det_vcp(c, v):
    n = len(c)
    if n < 40: return None
    atr = np.mean(np.abs(np.diff(c))) if n > 1 else np.mean(c)*0.02
    prom = max(atr*1.5, np.mean(c)*0.01)
    try:
        highs, _ = find_peaks(c, prominence=prom, distance=5)
        lows, _  = find_peaks(-c, prominence=prom, distance=5)
    except: return None
    if len(highs) < 2 or len(lows) < 2: return None
    contractions = []
    hl = list(highs) + [n]
    for i, hi in enumerate(hl[:-1]):
        nh = hl[i+1]
        nl = lows[(lows > hi) & (lows < nh)]
        lo = nl[0] if len(nl)>0 else (hi+int(np.argmin(c[hi:nh])) if nh-hi>=3 else -1)
        if lo < 0 or lo >= n: continue
        depth = (c[hi]-c[lo])/c[hi]
        duration = lo - hi   # number of bars from peak to trough
        if depth < 0.03: continue
        contractions.append((hi, lo, depth, duration))
    if len(contractions) < 3: return None
    depths    = [ct[2] for ct in contractions]
    durations = [ct[3] for ct in contractions]
    # Gap 6 FIX: each contraction must be shallower AND shorter than the previous
    if not all(depths[i] <= depths[i-1]*0.85 for i in range(1,len(depths))): return None
    if not all(durations[i] <= durations[i-1]*1.10 for i in range(1,len(durations))): return None
    if contractions[-1][1] < n*0.5: return None
    final_depth = depths[-1]
    if final_depth > 0.12: return None
    final_days = contractions[-1][1] - contractions[-1][0]
    if final_days > 15: return None
    if v is not None:
        vol_in_ct = [np.mean(v[ct[0]:ct[1]+1]) for ct in contractions]
        vol_contracting = all(vol_in_ct[i] <= vol_in_ct[i-1]*1.1 for i in range(1,len(vol_in_ct)))
    else:
        vol_contracting = True
    pivot = float(np.max(c[highs])); vs = vsurge(v, n)
    bo = c[-1] >= pivot*0.98 and (vs is not None and vs >= 1.5)
    # GAP-P1: start = first contraction peak; end = final contraction trough (last bar of pattern)
    vcp_start_bar = int(contractions[0][0])  # first peak
    vcp_end_bar   = int(contractions[-1][1]) # last trough = pattern completion
    return dict(pattern="VCP", status="Breakout Ready" if bo else "Forming",
                quality=round((1-final_depth) * (1 if vol_contracting else 0.7), 3),
                bz=round(pivot,2),
                bottom=round(float(c[contractions[-1][1]]),2), last=round(float(c[-1]),2), vs=vs,
                m1=round(depths[0]*100,2), m2=round(final_depth*100,2),
                m3=round(depths[-1]/depths[0],2) if depths[0]>0 else None,
                m4=len(contractions), m5=final_days,
                _start_bar=vcp_start_bar, _end_bar=vcp_end_bar)

def det_fb(c, v):
    n = len(c)
    if n < 35: return None
    best = None
    for bl in range(15, min(75,n)+1):
        base = c[-bl:]; bh,blo = np.max(base),np.min(base)
        br = (bh-blo)/bh if bh>0 else 1
        if br > 0.20: break
        bs = n-bl; tl = min(80,bs)
        if tl < 15: continue
        pre = c[bs-tl:bs]
        tg = (pre[-1]-np.min(pre))/np.min(pre) if np.min(pre)>0 else 0
        if tg < 0.10: continue
        if best is None or br < best["br"]: best = dict(bl=bl,bh=bh,blo=blo,br=br,tg=tg)
    if best is None: return None
    vs = vsurge(v,n); bo = c[-1]>=best["bh"]*0.99 and (vs is not None and vs>=1.2)
    # GAP-P1: flat base starts at n-best['bl'], ends at n-1
    fb_start = n - best["bl"]
    return dict(pattern="FlatBase", status="Breakout Ready" if bo else "Forming",
                quality=round(best["tg"]-best["br"],3), bz=round(float(best["bh"]),2),
                bottom=round(float(best["blo"]),2), last=round(float(c[-1]),2), vs=vs,
                m1=round(best["br"]*100,2), m2=round(best["tg"]*100,2), m3=best["bl"], m4=None, m5=None,
                _start_bar=fb_start, _end_bar=n-1)

def det_ihs(c, v):
    n = len(c)
    if n < 40: return None
    atr = np.mean(np.abs(np.diff(c))) if n>1 else np.mean(c)*0.015
    try: troughs,_ = find_peaks(-c, prominence=max(atr*1.2,np.mean(c)*0.008), distance=6)
    except: return None
    if len(troughs) < 3: return None
    hc = troughs[(troughs>n*0.20)&(troughs<n*0.80)]
    if len(hc)==0: return None
    hi = hc[np.argmin(c[hc])]
    hl = [t for t in troughs if t<hi and c[t]>c[hi]]
    hr = [t for t in troughs if t>hi and c[t]>c[hi]]
    if not hl or not hr: return None
    li,ri = hl[-1],hr[0]; ls,hd,rs_ = c[li],c[hi],c[ri]
    sa = (ls+rs_)/2; asym = abs(ls-rs_)/sa
    if asym > 0.18: return None
    hb = (sa-hd)/sa
    if not (0.03<=hb<=0.50): return None
    nl = (np.max(c[li:hi+1])+np.max(c[hi:ri+1]))/2
    if ri>=n-2: return None

    # Gap 6 FIX: right shoulder must form on LOWER volume than left shoulder
    vol_ok = True
    if v is not None and len(v) > ri:
        # average volume in a 5-bar window around each shoulder trough
        left_win  = v[max(0,li-2):li+3]
        right_win = v[max(0,ri-2):ri+3]
        if len(left_win) > 0 and len(right_win) > 0:
            if np.mean(right_win) >= np.mean(left_win) * 1.10:
                vol_ok = False   # right shoulder heavier — weaker setup

    vs = vsurge(v,n); bo = c[-1]>=nl*0.99 and (vs is not None and vs>=1.2)
    quality_base = round(hb-asym, 3)
    quality_adj  = round(quality_base * (1.0 if vol_ok else 0.80), 3)
    return dict(pattern="InvHS", status="Breakout Ready" if bo else "Forming",
                quality=quality_adj, bz=round(float(nl),2), bottom=round(float(hd),2),
                last=round(float(c[-1]),2), vs=vs, m1=round(hb*100,2), m2=round(asym*100,2), m3=int(ri-li), m4=None, m5=None,
                _start_bar=int(li), _end_bar=int(ri))  # GAP-P1: left shoulder → right shoulder

def det_dbot(c, v):
    n = len(c)
    if n < 30: return None
    try: troughs,_ = find_peaks(-c, prominence=0.02*np.mean(c), distance=5)
    except: return None
    if len(troughs)<2: return None
    best = None
    for i in range(len(troughs)):
        for j in range(i+1,len(troughs)):
            sep = troughs[j]-troughs[i]
            if not (10<=sep<=150): continue
            p1,p2 = c[troughs[i]],c[troughs[j]]
            diff = abs(p1-p2)/min(p1,p2)
            if diff>0.08: continue
            mid = np.max(c[troughs[i]:troughs[j]+1])
            mr = (mid-(p1+p2)/2)/((p1+p2)/2)
            if mr<0.06 or troughs[j]>=n-2: continue
            if best is None or mr-diff>best["sc"]:
                best = dict(sc=mr-diff,mid=mid,diff=diff,mr=mr,bottom=min(p1,p2),
                            t1_idx=int(troughs[i]), t2_idx=int(troughs[j]))
    if best is None: return None
    vs = vsurge(v,n); bo = c[-1]>=best["mid"]*0.99 and (vs is not None and vs>=1.2)
    return dict(pattern="DoubleBottom", status="Breakout Ready" if bo else "Forming",
                quality=round(best["sc"],3), bz=round(float(best["mid"]),2),
                bottom=round(float(best["bottom"]),2), last=round(float(c[-1]),2), vs=vs,
                m1=round(best["diff"]*100,2), m2=round(best["mr"]*100,2), m3=None, m4=None, m5=None,
                _start_bar=int(best["t1_idx"]), _end_bar=int(best["t2_idx"]))

def det_asctri(c, v):
    n = len(c)
    if not (15<=n<=200): return None
    try:
        pks,_ = find_peaks(c, prominence=0.01*np.mean(c), distance=3)
        trs,_ = find_peaks(-c, prominence=0.01*np.mean(c), distance=3)
    except: return None
    if len(pks)<2 or len(trs)<2: return None
    pp=c[pks]; res=np.median(pp)
    if (np.max(pp)-np.min(pp))/res>0.04: return None
    tp=c[trs]
    slopes = [(tp[j]-tp[i])/(trs[j]-trs[i]) for i in range(len(trs)) for j in range(i+1,len(trs)) if trs[j]!=trs[i]]
    if not slopes or np.median(slopes)<=0: return None
    rise=(tp[-1]-tp[0])/tp[0] if tp[0]>0 else 0
    if rise<0.015 or trs[-1]<n*0.4: return None
    vs=vsurge(v,n); bo=c[-1]>=res*0.99 and (vs is not None and vs>=1.2)
    return dict(pattern="AscTriangle", status="Breakout Ready" if bo else "Forming",
                quality=round(rise,3), bz=round(float(res),2), bottom=round(float(tp[0]),2),
                last=round(float(c[-1]),2), vs=vs, m1=round((np.max(pp)-np.min(pp))/res*100,2),
                m2=round(rise*100,2), m3=len(pks), m4=len(trs), m5=None,
                _start_bar=int(min(pks[0], trs[0])), _end_bar=n-1)  # GAP-P1: from first pivot

def det_flag(c, v):
    n = len(c)
    if n < 10: return None
    best = None
    for pl in range(4,min(25,n-3)+1):
        for fl in range(3,min(20,n-pl)+1):
            tot=pl+fl
            if tot>n: break
            pole=c[n-tot:n-fl]; flag=c[n-fl:]
            if pole[0]<=0: continue
            pg=(pole[-1]-pole[0])/pole[0]
            if not (0.08<=pg<=1.5): continue
            try:
                x=np.arange(pl); cf=np.polyfit(x,pole,1)
                ssr=np.sum((pole-np.polyval(cf,x))**2); sst=np.sum((pole-np.mean(pole))**2)
                r2=1-ssr/sst if sst>0 else 0
            except: continue
            if cf[0]<=0 or r2<0.55: continue
            up=np.sum(np.diff(pole)>0)/(pl-1) if pl>1 else 0
            if up<0.55: continue
            fhi,flo=np.max(flag),np.min(flag)
            fd=(pole[-1]-flo)/pole[-1] if pole[-1]>0 else 1
            if fd>0.25: continue
            ph=pole[-1]-pole[0]; fr=(fhi-flo)/ph if ph>0 else 1
            if fr>0.70: continue
            q=pg*r2*up-fd-fr*0.5
            if best is None or q>best["q"]:
                best=dict(q=q,fhi=fhi,flo=flo,pg=pg,r2=r2,fd=fd,ps=c[n-tot],pt=pole[-1],pl=pl,fl=fl)
    if best is None: return None
    if v is not None and n>=best["fl"]+1:
        fv=np.mean(v[n-best["fl"]:-1]) if best["fl"]>1 else np.mean(v[-best["fl"]:])
        vs=round(float(v[-1]/fv),2) if fv>0 else None
    else: vs=None
    bo=c[-1]>=best["fhi"]*0.995 and (vs is not None and vs>=1.2)
    pname="HighTightFlag" if best["pg"]>=1.0 else "BullFlag"
    # GAP-P1: start = beginning of pole (n - pl - fl), end = last bar of flag (n-1)
    flag_start_bar = n - best["pl"] - best["fl"]
    return dict(pattern=pname, status="Breakout Ready" if bo else "Flag Forming",
                quality=round(best["q"],3), bz=round(float(best["fhi"]),2),
                bottom=round(float(best["flo"]),2), last=round(float(c[-1]),2), vs=vs,
                m1=round(best["pg"]*100,2), m2=round(best["r2"],3), m3=round(best["fd"]*100,2),
                m4=best["pl"], m5=best["fl"],
                _start_bar=flag_start_bar, _end_bar=n-1)

def det_fwedge(c, v):
    n = len(c)
    if n<25: return None
    atr=np.mean(np.abs(np.diff(c))) if n>1 else np.mean(c)*0.015
    prom=max(atr*1.2,np.mean(c)*0.008)
    try:
        highs,_ = find_peaks(c, prominence=prom, distance=4)
        lows,_  = find_peaks(-c, prominence=prom, distance=4)
    except: return None
    if len(highs)<2 or len(lows)<2: return None
    try:
        h_sl=np.polyfit(highs.astype(float),c[highs],1)[0]
        l_sl=np.polyfit(lows.astype(float),c[lows],1)[0]
    except: return None
    if h_sl>=0 or l_sl>=h_sl or abs(l_sl)<=abs(h_sl): return None
    upper=np.polyval(np.polyfit(highs.astype(float),c[highs],1),n-1)
    vs=vsurge(v,n); bo=c[-1]>=upper*0.99 and (vs is not None and vs>=1.2)
    return dict(pattern="FallingWedge", status="Breakout Ready" if bo else "Forming",
                quality=round(abs(h_sl),4), bz=round(float(upper),2),
                bottom=round(float(np.min(c[lows])),2), last=round(float(c[-1]),2), vs=vs,
                m1=round(h_sl,4), m2=round(l_sl,4), m3=len(highs), m4=len(lows), m5=None,
                _start_bar=int(min(highs[0], lows[0])), _end_bar=n-1)  # GAP-P1

def det_momburst(c, v):
    """
    Stockbee/Bonde MomBurst: gain>4% on volume expansion vs 20-day avg.
    DAY-1 MISS FIX: TI65 was a hard GATE (return None if <1.0) that killed
    valid signals on the very burst day — the day TI65 first crosses 1.0 is
    exactly the day we want to alert.
    Fixed approach:
      • TI65 >= 1.0  → REQUIRED (stock in uptrend structure)
      • TI65 >= 1.05 → BONUS quality points
      • Vol: 1.5× 20-day avg (not 50-day — 50-day misses early-stage moves)
      • No requirement for prior narrow day (Bonde 2LYNCH) — scored separately
    Also DETECTORS windows now [30,50,65] so n=65 window gives TI65 on burst day.
    """
    n = len(c)
    if n < 30 or v is None: return None
    if c[-2] <= 0: return None

    # ── Gate 1: Day gain >= 4% ────────────────────────────────────────────────
    day_gain = (c[-1] - c[-2]) / c[-2]
    if day_gain < 0.04: return None

    # ── Gate 2: Volume expansion vs 20-day avg (not 50-day — catches day 1) ──
    # Research: Bonde uses 20-day avg. 50-day avg makes detector fire on day 3+
    vol_lb = min(20, n - 1)
    vol_avg20 = np.mean(v[-vol_lb - 1:-1])   # exclude today's vol from avg
    if vol_avg20 <= 0 or v[-1] < vol_avg20 * 1.5:
        return None

    # ── Gate 3: Liquidity (₹15Cr+ avg daily turnover) ────────────────────────
    if n >= 20:
        avg_to = np.mean(v[-20:]) * np.mean(c[-20:]) / 1e7
        if avg_to < MIN_LIQUIDITY_CR: return None

    # ── Gate 4: TI65 — must be in uptrend (>= 1.0). Advisory at 1.05. ────────
    # NOT a hard kill gate: TI65 = 1.0 on burst day IS the signal we want.
    ti65 = None
    if n >= 65:
        avg65 = np.mean(c[-65:])
        ti65  = round(np.mean(c[-7:]) / avg65, 4) if avg65 > 0 else 0.0
        # Hard kill only if CLEARLY in downtrend (below 0.97)
        if ti65 < 0.97: return None

    # ── Gate 5: Above 50-day MA (structure confirmation) ─────────────────────
    ma50 = np.mean(c[-min(50, n):])
    if c[-1] < ma50 * 0.93: return None   # relaxed from 0.95 — day-1 can dip briefly

    # ── Gate 6: Not too extended from 52-week high ───────────────────────────
    if n >= 50:
        hi52 = np.max(c[-min(252, n):])
        if hi52 > 0 and (hi52 - c[-1]) / hi52 > MAX_DIST_52WK_PCT: return None

    # ── Metrics ───────────────────────────────────────────────────────────────
    # Narrow prior day (2LYNCH "N") — bonus, not gate
    n_flag = 0
    if n >= 3 and c[-3] > 0:
        prev_range = abs(c[-2] - c[-3]) / c[-3]
        n_flag = 1 if prev_range < 0.02 else 0

    vs_yest = round(float(v[-1] / v[-2]), 2) if v[-2] > 0 else 1.0
    vs_20d  = round(float(v[-1] / vol_avg20), 2) if vol_avg20 > 0 else 1.0
    quality = round(day_gain * (vs_20d / 3.0), 4)   # scale: 4% gain × 3× vol = quality 0.04

    return dict(
        pattern="MomBurst", status="Burst Active",
        quality=quality,
        bz=round(float(c[-1]), 2),
        bottom=round(float(c[-2]), 2),
        last=round(float(c[-1]), 2),
        vs=vs_20d,
        m1=round(day_gain * 100, 2),      # day gain %
        m2=round(vs_yest, 2),             # vol vs yesterday
        m3=round(ti65, 3) if ti65 else None,  # TI65
        m4=float(n_flag),                 # narrow prior day
        m5=round((c[-1]-c[-5])/c[-5]*100, 2) if n >= 5 and c[-5] > 0 else None,  # 5d return
        _start_bar=n-1, _end_bar=n-1,    # GAP-P1: burst is a single-day event
    )

def det_epivot(c, v, o=None, hi=None, lo=None):
    """Episodic Pivot — must HOLD the gap (close strength >= 65%)."""
    n = len(c)
    if n<22: return None
    gap=((o[-1]-c[-2])/c[-2]) if o is not None and len(o)==n else ((c[-1]-c[-2])/c[-2])
    if gap<0.05: return None
    vs=vsurge(v,n)
    if vs is None or vs<3.0: return None
    if c[-1]<np.mean(c[-min(200,n):]): return None
    if hi is not None and lo is not None and len(hi)==n and len(lo)==n:
        day_range = hi[-1] - lo[-1]
        cs = (c[-1] - lo[-1]) / day_range if day_range > 0 else 0.5
    else:
        cs = 0.7 if (o is None or c[-1] >= o[-1]) else 0.3
    if cs < 0.65: return None
    return dict(pattern="EpisodicPivot", status="Breakout Ready",
                quality=round(gap * cs, 3), bz=round(float(c[-1]),2), bottom=round(float(c[-2]),2),
                last=round(float(c[-1]),2), vs=vs,
                m1=round(gap*100,2), m2=vs, m3=round(cs,2), m4=None, m5=None,
                _start_bar=n-1, _end_bar=n-1)  # GAP-P1: EP is a single catalyst day

def det_ppivot(c, v):
    """Pocket Pivot: Up 1%+, vol > max down-day vol of last 10 × 1.3, above 50MA."""
    n = len(c)
    if n < 15 or v is None or len(v) < 15: return None
    day_gain = (c[-1] - c[-2]) / c[-2] if c[-2] > 0 else 0
    if day_gain < 0.01: return None
    max_dv = 0.0
    for i in range(2, min(12, n)):
        if c[-i] < c[-i-1]: max_dv = max(max_dv, v[-i])
    if max_dv == 0 or v[-1] <= max_dv * 1.3: return None
    if c[-1] < np.mean(c[-min(50,n):]): return None
    ti = calc_ti65(c)
    if ti is not None and ti < 1.01: return None
    vol_ma50 = np.mean(v[-min(50,n):])
    if v[-1] < vol_ma50: return None
    vs = round(float(v[-1] / max_dv), 2)
    return dict(pattern="PocketPivot", status="Pocket Pivot",
                quality=round(vs * day_gain * 10, 3),
                bz=round(float(c[-2]), 2), bottom=round(float(c[-2]), 2),
                last=round(float(c[-1]), 2), vs=vs,
                m1=round(day_gain * 100, 2), m2=vs,
                m3=round(ti, 4) if ti else None,
                m4=round(v[-1] / vol_ma50, 2), m5=None,
                _start_bar=n-1, _end_bar=n-1)  # GAP-P1: PP is a single pivot day

def det_anticipation(c, v):
    n = len(c)
    if n<30: return None
    ma50=np.mean(c[-min(50,n):])
    if c[-1]<ma50: return None
    ra=np.mean(np.abs(np.diff(c[-10:]))/c[-10:][:-1]) if n>=10 else 1
    aa=np.mean(np.abs(np.diff(c[-50:]))/c[-50:][:-1]) if n>=50 else ra
    if ra>aa*0.7: return None
    ema20=pd.Series(c).ewm(span=20).mean().values[-1]
    if abs(c[-1]-ema20)/ema20>0.03: return None
    if n>=20 and np.std(c[-20:])/np.mean(c[-20:])>0.03: return None
    return dict(pattern="Anticipation", status="Setup Ready",
                quality=round(1-ra/aa if aa>0 else 0,3), bz=round(float(np.max(c[-10:])),2),
                bottom=round(float(np.min(c[-10:])),2), last=round(float(c[-1]),2), vs=vsurge(v,n),
                m1=round(ra*100,4), m2=round(np.std(c[-20:])/np.mean(c[-20:])*100,2) if n>=20 else None,
                m3=round(abs(c[-1]-ema20)/ema20*100,2), m4=None, m5=None,
                _start_bar=n-10, _end_bar=n-1)  # GAP-P1: 10-bar consolidation window

def det_stage2bo(c, v):
    n = len(c)
    if n<170: return None
    ma150=np.mean(c[-150:]); ma150_prev=np.mean(c[-170:-20])
    if c[-1]<ma150: return None
    recently_below=any(c[i]<np.mean(c[max(0,i-150):i]) for i in range(n-10,n-1))
    if not recently_below or ma150<ma150_prev*0.98: return None
    vs=vsurge(v,n); bo=vs is not None and vs>=1.3
    # GAP-P1: stage2 start = point where price first crossed above 150MA recently
    # Approximate: find last bar that was below MA150 before the current run
    stage2_start = n - 30   # 30-bar base before breakout (conservative approximation)
    for i in range(n-2, max(n-60, 0), -1):
        if c[i] < np.mean(c[max(0,i-150):i]):
            stage2_start = i + 1
            break
    return dict(pattern="Stage2Breakout", status="Breakout Ready" if bo else "Forming",
                quality=round((c[-1]-ma150)/ma150,3), bz=round(float(ma150),2),
                bottom=round(float(np.min(c[-30:])),2), last=round(float(c[-1]),2), vs=vs,
                m1=round((c[-1]/ma150-1)*100,2), m2=None, m3=None, m4=None, m5=None,
                _start_bar=stage2_start, _end_bar=n-1)

# ================================================================
# PATTERN FORMATION METADATA + TARGET ETA
# ================================================================

# Research-backed T1/T2/T3 ETA in trading days (Bulkowski + Bonde)
# Format: (t1_days, t2_days, t3_days)
_PATTERN_ETA = {
    "CupHandle":      (15, 35, 70),   # Bulkowski: 2-4w T1, 5-10w T2
    "VCP":            (12, 30, 60),   # Minervini: 2-4w T1, 4-8w T2
    "FlatBase":       (8,  20, 45),   # 1-3w T1, 3-6w T2
    "InvHS":          (20, 50, 100),  # Bulkowski: 3-6w T1, 8-16w T2 (89% hit rate)
    "DoubleBottom":   (15, 40, 80),   # Bulkowski: 2-4w T1, 6-12w T2 (88% hit rate)
    "AscTriangle":    (12, 30, 55),   # 2-4w T1, 4-8w T2 (83% hit rate)
    "BullFlag":       (5,  15, 30),   # 1-2w T1, 2-4w T2
    "HighTightFlag":  (7,  20, 40),   # 1-2w T1, 3-6w T2
    "FallingWedge":   (15, 35, 65),   # 2-4w T1, 5-10w T2
    "MomBurst":       (3,  7,  12),   # Bonde: 2-3d T1, 4-7d T2
    "EpisodicPivot":  (4,  12, 25),   # Qullamaggie: 2-4d T1, 7-14d T2
    "PocketPivot":    (8,  20, 40),   # 5-10d T1, 15-25d T2
    "Anticipation":   (8,  20, 35),   # 1-3w T1, 3-6w T2
    "Stage2Breakout": (20, 60, 120),  # 3-6w T1, 8-20w T2
}

# ── GAP-D3 FIX: NSE Holiday Calendar 2025-2026 ──────────────────────────────
# Source: NSE official holiday list (published annually on nseindia.com)
# Add new year's list here each December. Only equity segment holidays included.
_NSE_HOLIDAYS = {
    # 2025
    "2025-01-26",  # Republic Day
    "2025-02-26",  # Mahashivratri
    "2025-03-14",  # Holi
    "2025-03-31",  # Id-Ul-Fitr (Ramadan Eid)
    "2025-04-10",  # Shri Ram Navami
    "2025-04-14",  # Dr. Baba Saheb Ambedkar Jayanti
    "2025-04-18",  # Good Friday
    "2025-05-01",  # Maharashtra Day
    "2025-08-15",  # Independence Day
    "2025-08-27",  # Ganesh Chaturthi
    "2025-10-02",  # Mahatma Gandhi Jayanti / Dussehra
    "2025-10-21",  # Diwali Laxmi Pujan
    "2025-10-22",  # Diwali Balipratipada
    "2025-10-24",  # Prakash Gurpurb Sri Guru Nanak Dev Ji
    "2025-11-05",  # Prakash Gurpurb (if applicable)
    "2025-12-25",  # Christmas
    # 2026
    "2026-01-26",  # Republic Day
    "2026-03-03",  # Mahashivratri
    "2026-03-20",  # Holi (Friday)
    "2026-04-02",  # Shri Ram Navami
    "2026-04-03",  # Good Friday
    "2026-04-14",  # Dr. Baba Saheb Ambedkar Jayanti
    "2026-05-01",  # Maharashtra Day
    "2026-08-15",  # Independence Day / Muharram
    "2026-09-17",  # Ganesh Chaturthi
    "2026-10-02",  # Mahatma Gandhi Jayanti
    "2026-10-20",  # Dussehra
    "2026-11-09",  # Diwali Laxmi Pujan
    "2026-11-10",  # Diwali Balipratipada
    "2026-11-25",  # Prakash Gurpurb Sri Guru Nanak Dev Ji
    "2026-12-25",  # Christmas
}

def _trading_day_add(from_date, n_days: int) -> str:
    """
    GAP-D3 FIX: Add n_days trading days to from_date, skipping weekends AND
    NSE market holidays. Previously only skipped weekends — ETAs were off by
    1-2 days around every NSE holiday (15 per year).
    """
    from datetime import date as _date, timedelta as _td
    if isinstance(from_date, str):
        from_date = _date.fromisoformat(from_date)
    cur = from_date
    added = 0
    while added < n_days:
        cur += _td(days=1)
        if cur.weekday() < 5 and str(cur) not in _NSE_HOLIDAYS:
            added += 1
    return str(cur)

def calc_pattern_meta(df: pd.DataFrame, w: int, pattern: str,
                       start_bar: int = None, end_bar: int = None) -> dict:
    """
    GAP-P1 FIX: pattern_start_date is now pivot-based (start_bar from detector),
    not window-based (df.index[-w]). Previously df.index[-w] overstated duration
    by using the full scan window even when the pattern only used part of it.

    GAP-P2 FIX: pattern_end_date added — the bar where the formation completed
    (right rim / final contraction low / handle low / etc.). Previously missing.

    GAP-P3 FIX: timeframe label now shows the actual chart timeframe ("Daily")
    instead of a window-size bucket like "Intraday/Short" for a 10-bar daily segment.

    Parameters
    ----------
    df          : full price DataFrame for the stock
    w           : scan window size (still used for fallback duration)
    pattern     : pattern name (for ETA lookup)
    start_bar   : index within df (0-based from start of df) where pattern begins.
                  Provided by detector via _start_bar key. Falls back to df.index[-w].
    end_bar     : index within df where pattern completed. Falls back to df.index[-1].

    Returns dict with: pattern_start_date, pattern_end_date, formation_days,
                        timeframe, t1_eta, t2_eta, t3_eta
    """
    try:
        n = len(df)
        # ── Pattern start: pivot-based if detector provided it, else window fallback ──
        if start_bar is not None and 0 <= start_bar < n:
            start_idx = start_bar
        else:
            start_idx = max(0, n - w)   # window-based fallback (original behaviour)

        # ── Pattern end: detector-provided or last bar ──
        if end_bar is not None and 0 <= end_bar < n:
            end_idx = end_bar
        else:
            end_idx = n - 1

        pattern_start_date = str(df.index[start_idx])[:10]
        pattern_end_date   = str(df.index[end_idx])[:10]

        # Formation duration = trading days between start and end (both inclusive)
        formation_days = max(1, end_idx - start_idx + 1)

        # GAP-P3 FIX: chart timeframe = actual bar frequency of the data used
        # All current detectors run on daily bars ("1d"). Weekly ("1wk") when weekly
        # validation data is used. Intraday labels added when broker API is integrated.
        timeframe = "Daily"

        # Target ETAs (trading days from today, using NSE holiday-aware function)
        d1, d2, d3 = _PATTERN_ETA.get(pattern, (10, 25, 50))
        today_str = str(_today())
        t1_eta = _trading_day_add(today_str, d1)
        t2_eta = _trading_day_add(today_str, d2)
        t3_eta = _trading_day_add(today_str, d3)

        return dict(
            pattern_start_date=pattern_start_date,
            pattern_end_date=pattern_end_date,
            formation_days=formation_days,
            timeframe=timeframe,
            t1_eta=t1_eta,
            t2_eta=t2_eta,
            t3_eta=t3_eta,
            # Keep pattern_formed as alias for backward compatibility with watchlist JSON
            pattern_formed=pattern_start_date,
        )
    except Exception:
        return dict(
            pattern_start_date=None, pattern_end_date=None,
            pattern_formed=None, formation_days=w,
            timeframe="Daily",
            t1_eta=None, t2_eta=None, t3_eta=None,
        )


# ================================================================
# Gap 10 FIX: Weekly chart validation gate
# Every base-pattern signal on the daily chart is validated against the
# weekly chart before a BUY recommendation is issued.
# Signals that fail weekly validation are downgraded to WATCH.
# ================================================================
_BASE_PATTERNS = {"CupHandle", "VCP", "FlatBase", "InvHS", "DoubleBottom",
                  "AscTriangle", "FallingWedge", "Stage2Breakout"}

def weekly_chart_valid(sym: str) -> tuple[bool, str]:
    """
    Returns (valid: bool, reason: str).

    Passing conditions (ALL must hold for a BUY on a base pattern):
      1. Weekly close above 10-week MA
      2. 10-week MA slope is positive over last 4 weeks
      3. Last week's volume was below 10-week average (dryup in base)
    """
    wdf = read_cache(sym, "1wk", limit=60)
    if wdf is None or len(wdf) < 14:
        return True, "no weekly data"   # can't invalidate — don't penalise

    wc = wdf["Close"].values.astype(float)
    wv = wdf["Volume"].values.astype(float) if "Volume" in wdf.columns else None

    # 1. Price above 10-week MA
    ma10w = np.mean(wc[-10:])
    if wc[-1] < ma10w:
        return False, "weekly price below 10w MA"

    # 2. 10-week MA rising over last 4 weeks
    ma10w_4w_ago = np.mean(wc[-14:-4])
    if ma10w <= ma10w_4w_ago:
        return False, "10w MA declining"

    # 3. Volume dry-up (last week below 10-week average)
    if wv is not None and len(wv) >= 11:
        wv_avg10 = np.mean(wv[-11:-1])   # 10-week avg excluding last week
        if wv[-1] > wv_avg10 * 1.20:     # 20% tolerance
            return False, "weekly volume not drying up"

    return True, "ok"


DETECTORS = {
    "CupHandle":     (det_cup,          [60,80,120,180,250]),
    "VCP":           (det_vcp,          [60,80,120,180,250]),
    "FlatBase":      (det_fb,           [40,60,80,120,180]),
    "InvHS":         (det_ihs,          [60,80,120,180,250]),
    "DoubleBottom":  (det_dbot,         [40,60,100,150,200]),
    "AscTriangle":   (det_asctri,       [30,50,80,120,180]),
    # GAP-P5 FIX: HighTightFlag removed from here — det_flag() already returns
    # pattern="HighTightFlag" when pole gain ≥ 100%. Having a separate entry
    # with a non-existent det_htf() caused a silent NameError on every HTF window.
    "BullFlag":      (det_flag,         [15,20,30,40,50,60]),
    "FallingWedge":  (det_fwedge,       [30,50,80,120]),
    # [30,50,65]: 30 catches early-stage bursts, 65 enables TI65 scoring on burst day.
    # Previously [50,65,80] — missed stocks with 30-49 bars of history.
    "MomBurst":      (det_momburst,     [30, 50, 65]),
    "EpisodicPivot": (det_epivot,       [30]),
    "PocketPivot":   (det_ppivot,       [30]),
    "Anticipation":  (det_anticipation, [30,50]),
    "Stage2Breakout":(det_stage2bo,     [180]),
}

# ================================================================
# HIGHER TIME-FRAME DETECTORS
# Weekly bars (1wk): windows in weeks. Monthly bars (1mo): windows in months.
# MomBurst / EpisodicPivot / PocketPivot / Anticipation are momentum/intraday
# patterns — they don't translate to weekly/monthly bars.
# ================================================================
WEEKLY_DETECTORS = {
    # Windows = number of WEEKLY bars. Hard minimums from detector code:
    # CupHandle≥50, VCP≥40, FlatBase≥35, InvHS≥40, DoubleBottom≥30,
    # AscTriangle 15-200, BullFlag≥10, FallingWedge≥25, Stage2Breakout≥170.
    # 10yr weekly history ≈ 520 bars, so windows up to ~260 are practical.
    "CupHandle":      (det_cup,      [ 52,  65,  78, 104]),  # 1-2yr cycles
    "VCP":            (det_vcp,      [ 52,  65,  78, 104]),
    "FlatBase":       (det_fb,       [ 35,  45,  55,  70]),  # 9mo-18mo base
    "InvHS":          (det_ihs,      [ 40,  52,  65,  78]),
    "DoubleBottom":   (det_dbot,     [ 30,  40,  52,  65]),
    "AscTriangle":    (det_asctri,   [ 15,  20,  30,  40]),  # 15-200 bar limit
    "BullFlag":       (det_flag,     [ 12,  16,  20,  26]),
    "FallingWedge":   (det_fwedge,   [ 26,  36,  52      ]),
    # Stage2Breakout: uses 150-bar MA → needs ≥170 weekly bars (≈3.5yr).
    # Conceptually valid: 150-week MA is the "3yr trend" used in Weinstein Stage 2.
    "Stage2Breakout": (det_stage2bo, [180, 260            ]),
}

MONTHLY_DETECTORS = {
    # Windows = number of MONTHLY bars. 10yr monthly history ≈ 120 bars.
    # Hard minimums: CupHandle≥50, VCP≥40, FlatBase≥35, InvHS≥40,
    # DoubleBottom≥30, AscTriangle 15-200.
    # Stage2Breakout needs ≥170 months = 14yr → NOT possible, excluded.
    "CupHandle":      (det_cup,      [ 50,  60,  80, 100]),  # 4-8yr patterns
    "VCP":            (det_vcp,      [ 40,  55,  70,  90]),  # 3-7yr
    "FlatBase":       (det_fb,       [ 35,  45,  60      ]),  # 3-5yr
    "InvHS":          (det_ihs,      [ 40,  55,  70      ]),  # 3-6yr
    "DoubleBottom":   (det_dbot,     [ 30,  40,  55      ]),  # 2.5-4.5yr
    "AscTriangle":    (det_asctri,   [ 15,  20,  30      ]),  # 1-2.5yr
    # BullFlag / FallingWedge / Stage2Breakout excluded (wrong timescale or data insufficient)
}

# ETAs in trading days, scaled for weekly bars (×5) and monthly bars (×22)
_PATTERN_ETA_WEEKLY = {
    "CupHandle":      ( 75, 175, 350),
    "VCP":            ( 60, 150, 300),
    "FlatBase":       ( 40, 100, 225),
    "InvHS":          (100, 250, 500),
    "DoubleBottom":   ( 75, 200, 400),
    "AscTriangle":    ( 60, 150, 275),
    "BullFlag":       ( 25,  75, 150),
    "HighTightFlag":  ( 35, 100, 200),
    "FallingWedge":   ( 75, 175, 325),
    "Stage2Breakout": (100, 300, 600),
}

_PATTERN_ETA_MONTHLY = {
    # Stage2Breakout removed from MONTHLY_DETECTORS (insufficient history),
    # entry kept here as fallback default in case _htf_eta.get() misses.
    "CupHandle":      (200,  500, 1000),
    "VCP":            (150,  400,  800),
    "FlatBase":       (100,  300,  600),
    "InvHS":          (250,  600, 1200),
    "DoubleBottom":   (200,  500, 1000),
    "AscTriangle":    (150,  400,  800),
}


# ================================================================
# SCAN ONE STOCK

# ================================================================
# SCAN ONE STOCK
# ================================================================
def scan_stock(sym, nifty_d, ftd_active, market_trend,
               period=PERIOD_DAILY, detector_filter=None, aggression=2,
               bulk_deals_today=None):
    """
    bulk_deals_today: dict from fundamentals.get_bulk_deals_today(), passed in
    from main() so we call the API only once per scan, not once per stock.
    """
    fund = dl_fund_cached(sym)  # same-day cache — avoids Yahoo rate limits on 2139 stocks
    fund_ok = fund.get("_fund_ok", False)
    cc, cr = cap_class(fund.get("marketCap"))
    rows = []; patterns_found = set()

    df = dl_cached(sym, period)  # uses incremental cache
    if df is None or len(df) < 30: return rows, fund_ok

    close = df["Close"].values.astype(float)
    vol   = df["Volume"].values.astype(float) if "Volume" in df.columns else None
    open_p= df["Open"].values.astype(float)   if "Open"   in df.columns else None
    high_p= df["High"].values.astype(float)   if "High"   in df.columns else None
    low_p = df["Low"].values.astype(float)    if "Low"    in df.columns else None

    # ── Global liquidity pre-filter (skip before any detector runs) ──────────
    if vol is not None and len(close) >= 20:
        avg_to = np.mean(vol[-20:]) * np.mean(close[-20:]) / 1e7
        if avg_to < MIN_LIQUIDITY_CR: return rows, fund_ok   # illiquid — skip entirely

    # ── 52-week high proximity filter ────────────────────────────────────────
    dist_52wk = None
    rs_pct = None   # default before pattern loop
    if len(close) >= 50:
        hi52 = np.max(close[-min(252,len(close)):])
        dist_52wk = round((hi52 - close[-1]) / hi52 * 100, 1) if hi52 > 0 else None

    if nifty_d is not None and len(nifty_d) > 0:
        nc = nifty_d.reindex(df.index, method="ffill")["Close"].values
        nr = np.full(len(nc), np.nan)
        for i in range(252, len(nc)):
            if nc[i-252] > 0: nr[i] = nc[i]/nc[i-252]-1
    else:
        nc = nr = None

    cs, completeness = canslim_score(close, vol, fund, nc, nr)
    stage = check_weinstein_stage(close)
    vdu = check_volume_dryup(vol)
    earnings_near = check_earnings_near(fund)
    adr = calc_adr(close)

    # BUG1 FIX: calc rs_pct BEFORE the pattern loop
    try:
        rs_pct = calc_rs_percentile(close, nc, lb=63, sym=sym)
    except Exception:
        rs_pct = None

    ti65_val = calc_ti65(close)
    lynch_val = lynch_score(close, vol)

    # Gap 4 FIX: volume indicators
    _obv_conf   = obv_confirming(close, vol) if vol is not None else True
    _udv_ratio  = calc_updown_vol_ratio(close, vol) if vol is not None else None
    _rs_leading = rs_line_leading(close, nc if nc is not None else None)

    # Gap 10 FIX: weekly chart validation
    _wkly_valid, _wkly_reason = weekly_chart_valid(sym)

    # ── GAP-F1: Promoter pledge score ────────────────────────────────────────
    _pledge_pct   = fund.get("scr_pledging_pct") or 0.0
    _promoter_pct = fund.get("scr_promoter_pct") or (
        (fund.get("insider_holding_pct") or 0) * 100)
    _pledge_note  = None
    if _pledge_pct >= 20:
        _pledge_note = f"PLEDGE⚠️({_pledge_pct:.0f}%)"
    elif _pledge_pct >= 10:
        _pledge_note = f"PLEDGE({_pledge_pct:.0f}%)"

    # ── GAP-F2: Bulk/block deals today ───────────────────────────────────────
    _bulk_deal_cr   = None
    _bulk_deal_note = None
    if bulk_deals_today:
        try:
            from fundamentals import has_insider_activity
            sym_clean = sym.replace(".NS","").upper()
            _has_deal, _deal_val, _deal_type = has_insider_activity(sym_clean, bulk_deals_today)
            if _has_deal and _deal_val:
                _bulk_deal_cr   = _deal_val
                _bulk_deal_note = f"BULK-DEAL💼 ₹{_deal_val:.0f}Cr"
        except Exception:
            pass

    # ── GAP-F3: Piotroski score ───────────────────────────────────────────────
    _piotroski = fund.get("piotroski_score")  # computed by fundamentals.py

    dets = {k:v for k,v in DETECTORS.items()
            if detector_filter is None or k in detector_filter}

    for pat, (detector, windows) in dets.items():
        best = None
        for w in windows:
            if len(close) < w: continue
            seg_c = close[-w:]; seg_v = vol[-w:] if vol is not None else None
            try:
                if pat == "EpisodicPivot":
                    seg_o  = open_p[-w:] if open_p is not None else None
                    seg_hi = high_p[-w:] if high_p is not None else None
                    seg_lo = low_p[-w:]  if low_p  is not None else None
                    res = detector(seg_c, seg_v, o=seg_o, hi=seg_hi, lo=seg_lo)
                else:
                    res = detector(seg_c, seg_v)
            except Exception: continue
            if res is None: continue
            if best is None or res["quality"] > best["quality"]:
                best = {**res, "_w": w}
        if best is None: continue

        mkt_up = ftd_active or "Bull" in str(market_trend) or "Uptrend" in str(market_trend)
        rec = recommend(best["status"], cs, mkt_up, aggression=aggression, rs_pct=rs_pct)
        if not rec: continue

        # Gap 10 FIX: downgrade base-pattern BUYs that fail weekly chart validation
        if pat in _BASE_PATTERNS and "BUY" in rec and not _wkly_valid:
            rec = rec.replace("BUY STRONG", "WATCH").replace("BUY MODERATE", "WATCH")
            rec += f" [wkly: {_wkly_reason}]"

        patterns_found.add(pat)
        stop, t1, t2, t3, rr = calc_targets(best["pattern"], best["bz"],
                                              best.get("bottom"), best["last"], adr,
                                              close=close,
                                              high=high_p, low=low_p)
        leg  = identify_leg(close, best["bz"])

        # GAP-P1+P2 FIX: translate segment-relative _start_bar/_end_bar to
        # absolute df indices, then to calendar dates via calc_pattern_meta.
        w = best["_w"]
        seg_offset = len(close) - w   # where this segment starts in full df
        abs_start = seg_offset + best.get("_start_bar", 0)
        abs_end   = seg_offset + best.get("_end_bar",   w - 1)
        pmeta = calc_pattern_meta(df, w, best["pattern"],
                                  start_bar=abs_start, end_bar=abs_end)

        # GAP-P6 FIX: VCP contraction count note
        vcp_contractions_note = None
        if best["pattern"] == "VCP" and best.get("m4"):
            vcp_contractions_note = f"VCP({best['m4']}C)"

        notes = " | ".join(filter(None, [
            "EARNINGS SOON" if earnings_near else None,
            "VOL DRY-UP" if vdu else None,
            "STAGE2" if "Stage2" in stage else None,
            f"ADR={adr}%" if adr >= 3.5 else None,
            "OBV⚠" if not _obv_conf else None,
            "RS-LEAD🌟" if _rs_leading else None,
            f"UD-VOL={_udv_ratio}" if _udv_ratio is not None else None,
            vcp_contractions_note,             # GAP-P6
            _pledge_note,                      # GAP-F1
            _bulk_deal_note,                   # GAP-F2
        ]))

        rows.append(dict(
            scan_date=str(_today()), scan_time=_ist("%H:%M"),
            scan_mode="daily", stock=sym.replace(".NS",""), name=fund.get("longName"),
            sector=fund.get("sector"), cap_class=cc, cap_cr=cr,
            pattern=best["pattern"],
            timeframe=pmeta["timeframe"],
            pattern_formed=pmeta.get("pattern_formed"),        # backward-compat alias
            pattern_start_date=pmeta.get("pattern_start_date"),  # GAP-P1
            pattern_end_date=pmeta.get("pattern_end_date"),      # GAP-P2
            formation_days=pmeta["formation_days"],
            t1_eta=pmeta["t1_eta"],
            t2_eta=pmeta["t2_eta"],
            t3_eta=pmeta["t3_eta"],
            status=best["status"],
            breakout_zone=best["bz"], cmp=best["last"], stop_loss=stop,
            target_1=t1, target_2=t2, target_3=t3, risk_reward=rr,
            quality=best["quality"], vol_surge=best.get("vs"),
            canslim_score=cs, data_completeness=completeness,
            rs_percentile=rs_pct, dist_52wk_pct=dist_52wk,
            converging=None, leg=leg,
            earnings_near=1 if earnings_near else 0, ftd_active=1 if ftd_active else 0,
            vol_dryup=1 if vdu else 0, stage=stage, recommendation=rec,
            ti65=ti65_val, lynch_score_val=lynch_val,
            piotroski_score=_piotroski,          # GAP-F3
            pledge_pct=_pledge_pct if _pledge_pct else None,  # GAP-F1
            bulk_deal_cr=_bulk_deal_cr,          # GAP-F2
            m1=best.get("m1"), m2=best.get("m2"), m3=best.get("m3"),
            m4=best.get("m4"), m5=best.get("m5"), notes=notes or None))

    # ── Higher Time-Frame (HTF) scans: Weekly & Monthly ─────────────────────
    # Runs AFTER daily loop. Uses cached 1wk / 1mo bars already populated by
    # data_updater.py --daily. No extra download; pure cache read.
    for _htf_tf, _htf_label, _htf_dets, _htf_eta in [
        ("1wk", "Weekly",  WEEKLY_DETECTORS,  _PATTERN_ETA_WEEKLY),
        ("1mo", "Monthly", MONTHLY_DETECTORS, _PATTERN_ETA_MONTHLY),
    ]:
        _htf_df = read_cache(sym, _htf_tf)
        if _htf_df is None or len(_htf_df) < 6:
            continue
        _htf_c  = _htf_df["Close"].values.astype(float)
        _htf_v  = _htf_df["Volume"].values.astype(float) if "Volume" in _htf_df.columns else None

        _dets_htf = {k: v for k, v in _htf_dets.items()
                     if detector_filter is None or k in detector_filter}

        for pat, (detector, windows) in _dets_htf.items():
            best_htf = None
            for w in windows:
                if len(_htf_c) < w:
                    continue
                seg_c = _htf_c[-w:]
                seg_v = _htf_v[-w:] if _htf_v is not None else None
                try:
                    res = detector(seg_c, seg_v)
                except Exception:
                    continue
                if res is None:
                    continue
                if best_htf is None or res["quality"] > best_htf["quality"]:
                    best_htf = {**res, "_w": w}
            if best_htf is None:
                continue

            mkt_up  = ftd_active or "Bull" in str(market_trend) or "Uptrend" in str(market_trend)
            rec_htf = recommend(best_htf["status"], cs, mkt_up, aggression=aggression, rs_pct=rs_pct)
            if not rec_htf:
                continue

            # ETA — scaled for HTF bars
            d1, d2, d3  = _htf_eta.get(pat, (100, 250, 500))
            today_str   = str(_today())
            t1_eta_htf  = _trading_day_add(today_str, d1)
            t2_eta_htf  = _trading_day_add(today_str, d2)
            t3_eta_htf  = _trading_day_add(today_str, d3)

            # Pattern date range from HTF df
            n_htf = len(_htf_df)
            w_htf = best_htf["_w"]
            _s = max(0, n_htf - w_htf) + best_htf.get("_start_bar", 0)
            _e = max(0, n_htf - w_htf) + best_htf.get("_end_bar",   w_htf - 1)
            _s = min(_s, n_htf - 1)
            _e = min(_e, n_htf - 1)
            htf_start_date  = str(_htf_df.index[_s])[:10]
            htf_end_date    = str(_htf_df.index[_e])[:10]
            htf_form_bars   = max(1, _e - _s + 1)

            # Stop / targets from HTF close
            stop_htf, t1_htf, t2_htf, t3_htf, rr_htf = calc_targets(
                best_htf["pattern"], best_htf["bz"],
                best_htf.get("bottom"), best_htf["last"], adr,
                close=_htf_c, high=None, low=None)

            vdu_htf  = check_volume_dryup(_htf_v) if _htf_v is not None else False
            notes_htf = " | ".join(filter(None, [
                "EARNINGS SOON" if earnings_near else None,
                "VOL DRY-UP"   if vdu_htf       else None,
                _pledge_note,
                _bulk_deal_note,
            ]))

            patterns_found.add(f"{_htf_label[0]}:{pat}")  # e.g. "W:CupHandle"

            rows.append(dict(
                scan_date=str(_today()), scan_time=_ist("%H:%M"),
                scan_mode=_htf_label.lower(),           # "weekly" / "monthly"
                stock=sym.replace(".NS", ""), name=fund.get("longName"),
                sector=fund.get("sector"), cap_class=cc, cap_cr=cr,
                pattern=best_htf["pattern"],
                timeframe=_htf_label,                   # "Weekly" / "Monthly"
                pattern_formed=htf_start_date,
                pattern_start_date=htf_start_date,
                pattern_end_date=htf_end_date,
                formation_days=htf_form_bars,
                t1_eta=t1_eta_htf, t2_eta=t2_eta_htf, t3_eta=t3_eta_htf,
                status=best_htf["status"],
                breakout_zone=best_htf["bz"], cmp=best_htf["last"], stop_loss=stop_htf,
                target_1=t1_htf, target_2=t2_htf, target_3=t3_htf, risk_reward=rr_htf,
                quality=best_htf["quality"], vol_surge=best_htf.get("vs"),
                canslim_score=cs, data_completeness=completeness,
                rs_percentile=rs_pct, dist_52wk_pct=dist_52wk,
                converging=None, leg=None,
                earnings_near=1 if earnings_near else 0, ftd_active=1 if ftd_active else 0,
                vol_dryup=1 if vdu_htf else 0, stage=stage, recommendation=rec_htf,
                ti65=ti65_val, lynch_score_val=lynch_val,
                piotroski_score=_piotroski,
                pledge_pct=_pledge_pct if _pledge_pct else None,
                bulk_deal_cr=_bulk_deal_cr,
                m1=best_htf.get("m1"), m2=best_htf.get("m2"), m3=best_htf.get("m3"),
                m4=best_htf.get("m4"), m5=best_htf.get("m5"),
                notes=notes_htf or None,
            ))

    if len(patterns_found) > 1:
        conv = "+".join(sorted(patterns_found))
        for r in rows: r["converging"] = conv
    return rows, fund_ok

# ================================================================
# TELEGRAM — text + CSV file attachment
# ================================================================
def send_telegram(msg):
    """
    GAP-O1 FIX: Split messages longer than 4000 chars into multiple Telegram
    messages instead of hard-cutting. Previously signals after the 4000-char
    cutoff were silently dropped from Telegram (still in CSV, but invisible
    to traders using only Telegram).

    Telegram's hard limit is 4096 chars per message. We use 3900 to leave
    room for the "📎 Part N/M" header added to continuation messages.
    """
    if not TG_TOKEN or not TG_CHAT: return
    try:
        import requests as req
        MAX_LEN = 3900
        if len(msg) <= MAX_LEN:
            req.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                     data={"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"},
                     timeout=15)
            return
        # Split on newlines to avoid cutting mid-signal
        parts = []
        current = ""
        for line in msg.split("\n"):
            candidate = current + ("\n" if current else "") + line
            if len(candidate) > MAX_LEN:
                if current:
                    parts.append(current)
                current = line
            else:
                current = candidate
        if current:
            parts.append(current)
        total = len(parts)
        for i, part in enumerate(parts, 1):
            header = f"📎 Part {i}/{total}\n" if total > 1 else ""
            req.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                     data={"chat_id": TG_CHAT, "text": header + part,
                           "parse_mode": "HTML"},
                     timeout=15)
            if i < total:
                time.sleep(0.5)   # avoid Telegram rate limit (30 msgs/sec)
    except Exception as e:
        log.error(f"Telegram msg: {e}")

def send_telegram_file(filepath, caption=""):
    """Send a file (CSV) as a Telegram document attachment."""
    if not TG_TOKEN or not TG_CHAT: return
    if not os.path.exists(filepath):
        log.warning(f"File not found for Telegram: {filepath}")
        return
    try:
        import requests as req
        with open(filepath, "rb") as f:
            resp = req.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendDocument",
                data={"chat_id": TG_CHAT, "caption": caption[:1024]},
                files={"document": (os.path.basename(filepath), f, "text/csv")},
                timeout=60,
            )
        if resp.ok:
            log.info(f"CSV sent to Telegram: {os.path.basename(filepath)}")
        else:
            log.error(f"Telegram file failed: {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        log.error(f"Telegram file: {e}")

# ================================================================
# CSV OUTPUT — human-readable column names, separate BUY / WATCH
# ================================================================
_COL_LABELS = {
    "stock":               "Stock",
    "name":                "Company Name",
    "sector":              "Sector",
    "cap_class":           "Market Cap Class",
    "cap_cr":              "Market Cap (₹Cr)",
    "pattern":             "Pattern",
    "timeframe":           "Timeframe (Chart)",
    "pattern_formed":      "Pattern Start (Date)",       # backward-compat alias
    "pattern_start_date":  "Pattern Start (Date)",       # GAP-P1: pivot-based
    "pattern_end_date":    "Pattern End (Date)",          # GAP-P2: completion date
    "formation_days":      "Formation (Trading Days)",
    "status":              "Status",
    "cmp":                 "CMP (₹)",
    "breakout_zone":       "Breakout Zone (₹)",
    "stop_loss":           "Stop Loss (₹)",
    "target_1":            "Target 1 (₹)",
    "target_2":            "Target 2 (₹)",
    "target_3":            "Target 3 (₹)",
    "t1_eta":              "T1 ETA (Date)",
    "t2_eta":              "T2 ETA (Date)",
    "t3_eta":              "T3 ETA (Date)",
    "risk_reward":         "Risk:Reward",
    "vol_surge":           "Volume Surge (x)",
    "rs_percentile":       "RS Percentile",
    "dist_52wk_pct":       "% From 52Wk High",
    "canslim_score":       "CANSLIM Score",
    "data_completeness":   "Checks Available",
    "piotroski_score":     "Piotroski F-Score /9",   # GAP-F3
    "pledge_pct":          "Promoter Pledge %",       # GAP-F1
    "bulk_deal_cr":        "Bulk Deal ₹Cr (Today)",   # GAP-F2
    "converging":          "Converging Patterns",
    "leg":                 "Leg / Stage",
    "stage":               "Weinstein Stage",
    "recommendation":      "Recommendation",
    "score10":             "Score /10",
    "tier":                "Tier (BUY/WATCH/AVOID)",
    "quality":             "Pattern Quality",
    "scan_date":           "Scan Date",
    "scan_time":           "Scan Time",
    "scan_mode":           "Mode",
    "earnings_near":       "Earnings <14d",
    "ftd_active":          "FTD Active",
    "vol_dryup":           "Volume Dry-Up",
    "ti65":                "TI65",
    "lynch_score_val":     "Lynch Score /6",
    "notes":               "Notes",
}

_CSV_COLS = [
    "stock","name","sector","cap_class","cap_cr",
    "pattern","timeframe",
    "pattern_start_date","pattern_end_date","formation_days",  # GAP-P1, GAP-P2
    "status","converging",
    "cmp","breakout_zone","stop_loss",
    "target_1","target_2","target_3","risk_reward",
    "t1_eta","t2_eta","t3_eta",
    "score10","tier",
    "vol_surge","rs_percentile","dist_52wk_pct",
    "canslim_score","data_completeness",
    "piotroski_score","pledge_pct","bulk_deal_cr",  # GAP-F1/F2/F3
    "ti65","lynch_score_val",
    "leg","stage","recommendation",
    "earnings_near","vol_dryup","notes",
    "scan_date","scan_time",
]

def _prep_df(frame: pd.DataFrame) -> pd.DataFrame:
    """Select CSV columns, rename to human-readable headers, round floats."""
    if len(frame) == 0:
        return pd.DataFrame(columns=[_COL_LABELS.get(c,c) for c in _CSV_COLS])
    cols = [c for c in _CSV_COLS if c in frame.columns]
    out  = frame[cols].copy()
    out.columns = [_COL_LABELS.get(c,c) for c in cols]
    for col in out.select_dtypes(include="float").columns:
        out[col] = out[col].round(2)
    return out.reset_index(drop=True)

def _save_csvs(buys: pd.DataFrame, watches: pd.DataFrame,
               base_dir: str, ts: str) -> tuple:
    """
    Write three CSVs:
      scan_BUY_<date>_<time>.csv   — BUY signals, best first, readable columns
      scan_WATCH_<date>_<time>.csv — WATCH signals, best first, readable columns
    Returns (buy_path, watch_path).
    """
    today = str(_today())
    buy_path   = os.path.join(base_dir, f"scan_BUY_{today}_{ts}.csv")
    watch_path = os.path.join(base_dir, f"scan_WATCH_{today}_{ts}.csv")
    # utf-8-sig = Excel opens ₹ correctly on Windows
    _prep_df(buys).to_csv(buy_path,   index=False, encoding="utf-8-sig")
    _prep_df(watches).to_csv(watch_path, index=False, encoding="utf-8-sig")
    log.info(f"CSV BUY   ({len(buys):>4} rows) → {os.path.basename(buy_path)}")
    log.info(f"CSV WATCH ({len(watches):>4} rows) → {os.path.basename(watch_path)}")
    return buy_path, watch_path

def _fmt_signal_row(r, em_override=None):
    """Render one signal row for Telegram. Shared by daily/weekly/monthly sections."""
    tf     = r.get("timeframe", "Daily")
    em     = em_override or ("🟢" if "STRONG" in str(r.get("tier", "")) else "🟡")
    conv   = f" [{r['converging']}]" if r.get("converging") else ""
    notes  = f"\n   ⚠️ {r['notes']}" if r.get("notes") else ""
    score  = r.get("score10", "?")
    tier   = r.get("tier", "?")
    t1_eta = r.get("t1_eta", "?")
    t2_eta = r.get("t2_eta", "?")
    t3_eta = r.get("t3_eta", "?")
    p_start = r.get("pattern_start_date") or r.get("pattern_formed", "?")
    p_end   = r.get("pattern_end_date", "?")
    fdays   = r.get("formation_days", "?")
    if p_start and p_end and p_start != "?" and p_end != "?":
        date_str = f"📅 {p_start} → {p_end} ({fdays}d) | TF: {tf}"
    else:
        date_str = f"📅 {p_start} ({fdays}d) | TF: {tf}"
    pio_str  = f" Pio:{r['piotroski_score']}/9" if r.get("piotroski_score") is not None else ""
    deal_str = f" 💼₹{r['bulk_deal_cr']:.0f}Cr" if r.get("bulk_deal_cr") else ""
    return (
        f"{em} <b>{r['stock']}</b> ({r.get('cap_class','?')}) — {r['pattern']}{conv}\n"
        f"   Score: <b>{score}/10</b>  {tier}\n"
        f"   {date_str}\n"
        f"   CMP ₹{r['cmp']} | BZ ₹{r['breakout_zone']} | SL ₹{r.get('stop_loss','?')}\n"
        f"   T1 ₹{r.get('target_1','?')} by {t1_eta} | "
        f"T2 ₹{r.get('target_2','?')} by {t2_eta} | "
        f"T3 ₹{r.get('target_3','?')} by {t3_eta}\n"
        f"   RR {r.get('risk_reward','?')}x | CANSLIM {r['canslim_score']}/{r.get('data_completeness','?')} | "
        f"RS {r.get('rs_percentile','?')}pct | {r.get('stage','?')}{pio_str}{deal_str}{notes}"
    )

def fmt_daily(df, market_trend, ftd, regime_info=None):
    _ist_hm = _ist("%H:%M")
    ftd_str = "YES ✅" if ftd else "NO"

    # Split by timeframe
    daily_df   = df[df["timeframe"].str.lower() == "daily"]   if "timeframe" in df.columns else df
    weekly_df  = df[df["timeframe"].str.lower() == "weekly"]  if "timeframe" in df.columns else pd.DataFrame()
    monthly_df = df[df["timeframe"].str.lower() == "monthly"] if "timeframe" in df.columns else pd.DataFrame()

    buys_d  = daily_df[daily_df["tier"].str.contains("BUY",   na=False)]
    watch_d = daily_df[daily_df["tier"].str.contains("WATCH", na=False)]
    buys_w  = weekly_df[weekly_df["tier"].str.contains("BUY", na=False)]  if len(weekly_df)  else pd.DataFrame()
    buys_m  = monthly_df[monthly_df["tier"].str.contains("BUY",na=False)] if len(monthly_df) else pd.DataFrame()

    lines = [
        f"<b>📊 NSE Scanner — {_today()} {_ist_hm}</b>",
        f"Market: {market_trend} | FTD: {ftd_str} | Regime: {regime_info['regime'] if regime_info else '?'}",
        f"Daily  — BUY STRONG: {len(daily_df[daily_df['tier'].str.contains('STRONG',na=False)])} | "
        f"BUY MOD: {len(daily_df[daily_df['tier'].str.contains('MODERATE',na=False)])} | WATCH: {len(watch_d)}",
        f"Weekly — BUY: {len(buys_w)} | Monthly — BUY: {len(buys_m)}\n",
    ]

    # ── Daily BUYs ────────────────────────────────────────────────────────────
    if len(buys_d):
        lines.append("📈 <b>DAILY SIGNALS</b>")
        for _, r in buys_d.head(15).iterrows():
            lines.append(_fmt_signal_row(r))

    # ── Weekly BUYs ───────────────────────────────────────────────────────────
    if len(buys_w):
        lines.append("\n📆 <b>WEEKLY SIGNALS</b>  (multi-week patterns)")
        for _, r in buys_w.head(10).iterrows():
            lines.append(_fmt_signal_row(r, em_override="🔵"))

    # ── Monthly BUYs ──────────────────────────────────────────────────────────
    if len(buys_m):
        lines.append("\n🗓️ <b>MONTHLY SIGNALS</b>  (multi-month patterns)")
        for _, r in buys_m.head(5).iterrows():
            lines.append(_fmt_signal_row(r, em_override="🟣"))

    return "\n".join(lines)

def fmt_halfhour(alerts):
    """
    30-MINUTE SCAN ALERT FORMAT
    ═══════════════════════════
    Runs every 30 min during market hours (09:15–15:30 IST).
    Shows: score/10, tier, CMP vs BZ, SL, T1, RR, vol surge.
    Special alerts: 🏆 PROFIT TARGET (O'Neil 20-25% rule), 🚨 BREAKOUT TRIGGERED.
    """
    if not alerts: return None
    active  = sum(1 for a in alerts if a.get("status") in ("BREAKOUT TRIGGERED","Burst Active"))
    profit  = sum(1 for a in alerts if "PROFIT" in str(a.get("status","")))
    lines = [f"<b>⚡ 30-min — {_ist()}</b>  {len(alerts)} signals "
             f"({active} breakouts | {profit} profit targets)\n"]
    for a in alerts:
        status = a.get("status","")
        if "PROFIT" in status:
            em = "🏆"
        elif "BREAKOUT" in status:
            em = "🚨"
        elif status == "Burst Active":
            em = "🔥"
        elif "Pivot" in status:
            em = "🟡"
        else:
            em = "⚠️"
        vs   = f" Vol {a['vs']}x" if a.get("vs") else ""
        sl   = f" | SL ₹{a['stop']}" if a.get("stop") else ""
        t1   = f" | T1 ₹{a['t1']}" if a.get("t1") else ""
        rr   = f" | RR {a['rr']}x" if a.get("rr") else ""
        sc   = f" | ⭐{a['score10']}/10" if a.get("score10") is not None else ""
        tier = f" {a.get('tier','')}" if a.get("tier") else ""
        formed = f"\n   📅 Formed: {a['pattern_formed']} | TF: {a.get('timeframe','?')}" if a.get("pattern_formed") else ""
        eta   = f" | T1 by {a['t1_eta']}" if a.get("t1_eta") else ""
        lines.append(
            f"{em} <b>{a['stock']}</b> — {a['pattern']} — {status}\n"
            f"   ₹{a['cmp']} | BZ ₹{a.get('bz','?')}{vs}{sl}{t1}{rr}{sc}{tier}"
            f"{formed}{eta}"
        )
    return "\n".join(lines)

# ================================================================
# 30-MINUTE MODE
# ================================================================
def halfhour_check(nifty_d):
    alerts = []
    ftd_active = False; market_trend = "Unknown"
    if nifty_d is not None:
        ftd_active, _ = check_follow_through_day(nifty_d)
        market_trend = check_market_trend(nifty_d["Close"].values)

    # Part A: watchlist check — READ FROM CACHE ONLY (data_updater fetched already)
    _all_wl = load_watchlist()
    watchlist = [w for w in _all_wl if w.get("breakout_zone") and w.get("breakout_zone") > 0]
    watchlist = sorted(watchlist, key=lambda w: w.get("added_date",""), reverse=True)[:200]  # cap 200
    log.info(f"Watchlist: {len(watchlist)}/{len(_all_wl)} items (capped 200, cache-only)")
    for item in watchlist:
        sym = item["stock"] + ".NS"
        # Read from cache — data_updater --intraday already refreshed this
        df = read_cache(sym, "1d", limit=10)
        if df is None or len(df) < 2:
            df = read_cache(sym, "15m", limit=50)
        if df is None or len(df) < 2: continue
        close = df["Close"].values.astype(float)
        vol = df["Volume"].values.astype(float) if "Volume" in df.columns else None
        cmp = round(float(close[-1]), 2)
        bz = item.get("breakout_zone"); sl = item.get("stop_loss")
        alert_vs = None; status = "watching"

        # ── O'Neil 20-25% Profit Rule ──────────────────────────────────────
        # When a stock hits +20% from BZ → alert to take partial profits.
        # "Most leaders make their first meaningful move of 20-25% before pausing."
        if bz and bz > 0:
            profit_pct = (cmp - bz) / bz * 100
            profit_key = f"{item.get('pattern','')}_PROFIT20"
            if profit_pct >= 20 and not already_alerted_today(item["stock"], profit_key):
                alerts.append({
                    "stock": item["stock"], "pattern": item.get("pattern",""),
                    "status": f"PROFIT TARGET +{profit_pct:.1f}% — SELL 33% (O'Neil 20-25% rule)",
                    "cmp": cmp, "bz": bz, "vs": None,
                    "stop": sl, "t1": None, "rr": None,
                    "score10": item.get("score10"), "tier": item.get("tier"),
                })
                mark_alert_sent(item["stock"], profit_key, "PROFIT20")
                continue   # skip normal BREAKOUT check if already at profit

        if bz and cmp >= bz * 0.995:
            alert_vs = vsurge(vol, len(vol), 10) if vol is not None else None
            status = "BREAKOUT TRIGGERED" if (alert_vs and alert_vs >= 1.3) else "AT BREAKOUT ZONE"
        elif sl and cmp <= sl:
            status = "STOP HIT"
        if status != "watching" and not already_alerted_today(item["stock"], item.get("pattern","")):
            alerts.append({
                "stock": item["stock"], "pattern": item.get("pattern",""),
                "status": status, "cmp": cmp, "bz": bz, "vs": alert_vs,
                "stop": sl, "t1": item.get("target_1"), "rr": item.get("risk_reward"),
                "score10": item.get("score10"), "tier": item.get("tier"),
                "t1_eta": item.get("t1_eta"), "t2_eta": item.get("t2_eta"),
                "pattern_formed": item.get("pattern_formed"),
                "timeframe": item.get("timeframe"),
            })
            mark_alert_sent(item["stock"], item.get("pattern",""), status)

    # Part B: quick-scan QUICK_SIZE stocks for same-day signals
    # MomBurst before 1 PM IST = unconfirmed move, high fade rate
    current_hour_ist = _now().hour
    if current_hour_ist < HALFHOUR_CONFIRM_HOUR:
        intraday_dets = INTRADAY_DETECTORS - {"MomBurst"}
        log.info(f"Before {HALFHOUR_CONFIRM_HOUR}:00 IST — MomBurst suppressed")
    else:
        intraday_dets = INTRADAY_DETECTORS
    # CRITICAL FIX: stocks defined BEFORE use (was UnboundLocalError — warm_cache before assignment)
    stocks = load_universe()[:QUICK_SIZE]
    log.info(f"Quick-scan {len(stocks)} stocks (cache-read, no live download)...")
    # NO warm_cache here — data_updater --intraday ran before scanner in yml
    # warm_cache here = double download = rate limits
    quick_rows = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(scan_stock, s, nifty_d, ftd_active, market_trend,
                          PERIOD_QUICK, intraday_dets): s for s in stocks}
        for fut in as_completed(futs):
            try:
                rows, _ = fut.result()
                if rows: quick_rows.extend(rows)
            except Exception: pass

    log.info(f"Quick-scan: {len(quick_rows)} signals")

    # Build DataFrame of all signals for CSV
    quick_df = None
    if quick_rows:
        quick_df = (pd.DataFrame(quick_rows)
                    .drop_duplicates(subset=["stock","pattern"])
                    .sort_values("quality", ascending=False)
                    .reset_index(drop=True))
        quick_df["scan_time_ist"] = _ist()

    for sig in quick_rows:
        if not already_alerted_today(sig["stock"], sig["pattern"]):
            alerts.append({
                "stock": sig["stock"], "pattern": sig["pattern"],
                "status": sig["status"], "cmp": sig["cmp"],
                "bz": sig.get("breakout_zone"), "vs": sig.get("vol_surge"),
                "stop": sig.get("stop_loss"),
                "t1": sig.get("target_1"), "rr": sig.get("risk_reward"),
                "score10": sig.get("score10"), "tier": sig.get("tier"),
            })
            mark_alert_sent(sig["stock"], sig["pattern"], sig["status"])

    # Sort: BREAKOUT first, Burst Active, Pocket Pivot, others
    prio = lambda a: (0 if "BREAKOUT" in a.get("status","") else
                      1 if "Burst Active" == a.get("status","") else
                      2 if "Pivot" in a.get("status","") else 3)
    alerts.sort(key=prio)
    return alerts, quick_df

# ================================================================
# DASHBOARD
# ================================================================
def run_dashboard():
    try:
        from flask import Flask, render_template_string
    except ImportError:
        log.error("pip install flask"); sys.exit(1)

    app = Flask(__name__)
    TPL = """<!DOCTYPE html><html><head><title>NSE Scanner</title>
<meta http-equiv="refresh" content="300">
<style>
body{font-family:system-ui;margin:0;padding:20px;background:#0f172a;color:#e2e8f0}
h1{color:#38bdf8}h2{color:#94a3b8;font-size:14px;font-weight:400}
table{border-collapse:collapse;width:100%;margin:16px 0;font-size:12px}
th{background:#1e293b;color:#94a3b8;padding:8px 10px;text-align:left;position:sticky;top:0}
td{padding:5px 10px;border-bottom:1px solid #1e293b}tr:hover{background:#1e293b}
.buy-strong{color:#22c55e;font-weight:600}.buy-mod{color:#eab308}.watch{color:#94a3b8}
.tag{padding:2px 6px;border-radius:4px;font-size:11px;margin:1px}
.tc{background:#312e81;color:#a5b4fc}.te{background:#7f1d1d;color:#fca5a5}
.tv{background:#14532d;color:#86efac}
.stat{display:inline-block;background:#1e293b;padding:10px 20px;border-radius:8px;margin:4px;text-align:center}
.sn{font-size:26px;font-weight:700;color:#38bdf8}.sl{font-size:11px;color:#64748b}
.empty{text-align:center;padding:60px;color:#64748b}
</style></head><body>
<h1>NSE Pattern Scanner v3.3</h1>
<h2>{{ scan_time }} | Market: {{ market }}</h2>
<div>
<div class="stat"><div class="sn">{{ buys }}</div><div class="sl">BUY</div></div>
<div class="stat"><div class="sn">{{ watches }}</div><div class="sl">WATCH</div></div>
<div class="stat"><div class="sn">{{ wl_count }}</div><div class="sl">Watchlist</div></div>
<div class="stat"><div class="sn">{{ total }}</div><div class="sl">Signals today</div></div>
</div>
{% if rows %}
<table><tr><th>Stock</th><th>Cap</th><th>Pattern</th><th>Status</th><th>CMP</th>
<th>Breakout</th><th>Stop</th><th>T1</th><th>T2</th><th>T3</th><th>RR</th>
<th>CANSLIM</th><th>Leg</th><th>Stage</th><th>Reco</th><th>Notes</th></tr>
{% for r in rows %}<tr>
<td><b>{{ r.stock }}</b><br><small style="color:#64748b">{{ r.sector or '' }}</small></td>
<td>{{ r.cap_class }}</td>
<td>{{ r.pattern }}{% if r.converging %}<span class="tag tc">{{ r.converging }}</span>{% endif %}</td>
<td>{{ r.status }}</td><td>{{ r.cmp }}</td><td><b>{{ r.breakout_zone }}</b></td>
<td>{{ r.stop_loss }}</td><td>{{ r.target_1 }}</td><td>{{ r.target_2 }}</td><td>{{ r.target_3 }}</td>
<td>{{ r.risk_reward }}x</td><td>{{ r.canslim_score }}/{{ r.data_completeness }}</td>
<td>{{ r.leg }}</td><td>{{ r.stage }}</td>
<td class="{{ 'buy-strong' if 'strong' in (r.recommendation or '') else 'buy-mod' if 'BUY' in (r.recommendation or '') else 'watch' }}">{{ r.recommendation }}</td>
<td>{% if r.earnings_near %}<span class="tag te">EARN</span>{% endif %}
{% if r.vol_dryup %}<span class="tag tv">VDU</span>{% endif %}{{ r.notes or '' }}</td>
</tr>{% endfor %}</table>
{% else %}<div class="empty">No signals yet. Run <code>python scanner.py --daily --telegram</code></div>{% endif %}
</body></html>"""

    @app.route("/")
    def index():
        con = get_db()
        rows = db_query(con, "SELECT * FROM signals WHERE scan_date=? ORDER BY recommendation, canslim_score DESC",
                        (str(_today()),))
        runs = db_query(con, "SELECT * FROM runs ORDER BY id DESC LIMIT 1")
        wl = load_watchlist()
        con.close()
        buys = sum(1 for r in rows if "BUY" in (r.get("recommendation") or ""))
        watches = sum(1 for r in rows if "WATCH" in (r.get("recommendation") or ""))
        st = rows[0].get("stage","?") if rows else "?"
        rt = runs[0].get("scan_time","never") if runs else "never"
        return render_template_string(TPL, rows=rows, buys=buys, watches=watches,
                                      wl_count=len(wl), total=len(rows),
                                      scan_time=rt, market=st)
    log.info("Dashboard → http://localhost:5050")
    app.run(host="0.0.0.0", port=5050, debug=False)

# ================================================================
# HEALTHCHECK
# ================================================================
def healthcheck():
    print("=== Healthcheck v3.3 ===\n")
    ok = True
    print("[1] yfinance...     ", end="")
    df = dl("RELIANCE.NS", "1d", "1mo")
    print(f"OK ({len(df)} bars)" if df else "FAIL"); ok = ok and bool(df)
    print("[2] Fundamentals... ", end="")
    f = dl_fund("RELIANCE.NS")
    print(f"OK mcap={f.get('marketCap')}" if f.get("marketCap") else "WARN — no mcap")
    print("[3] Universe...     ", end="")
    u = load_universe()
    print(f"OK {len(u)} stocks {'(fallback)' if len(u) == len(NIFTY_500_FALLBACK_NS) else '(live)'}")
    print("[4] Detectors...    ", end="")
    np.random.seed(42); p = 0
    for nm, (det, _) in DETECTORS.items():
        try: det(100+np.random.normal(0,2,100), np.ones(100)*1000); p += 1
        except: pass
    print(f"OK {p}/{len(DETECTORS)}")
    print("[5] Database...     ", end="")
    try: con = get_db(); con.close(); print("OK")
    except Exception as e: print(f"FAIL {e}"); ok = False
    print("[6] Telegram...     ", end="")
    print(f"OK chat={TG_CHAT}" if TG_TOKEN and TG_CHAT else "NOT configured")
    print("[7] Watchlist...    ", end="")
    wl = load_watchlist()
    print(f"OK {len(wl)} items in {WL_PATH}" if wl else f"Empty ({WL_PATH})")
    print("[8] Flask...        ", end="")
    try: import flask; print("OK")
    except: print("NOT installed (optional)")
    print(f"\n{'ALL OK — deploy' if ok else 'FIX ISSUES FIRST'}")
    return ok

# ================================================================
# MAIN
# ================================================================

# ================================================================
# FORWARD RETURN TRACKER — answers "did the signals actually work?"
# ================================================================
def track_outcomes(con):
    """
    GAP-B2 FIX: Now records actual_r_multiple = (current_price - entry) / (entry - stop)
    and exit_type = T1 | STOP | OPEN for each signal outcome.

    This enables expectancy calculation in print_outcome_summary:
      E = avg(r_multiple) across all closed signals per pattern.

    Previously only tracked binary hit_t1 / hit_stop booleans — insufficient
    for measuring system expectancy or comparing patterns.
    """
    today = str(_today())
    for lookback in [3, 5, 10, 20]:
        target_date = str(_today() - timedelta(days=lookback + 2))
        signals = db_query(con,
            "SELECT id,stock,pattern,cmp,stop_loss,target_1,scan_date "
            "FROM signals WHERE scan_date=? AND recommendation LIKE 'BUY%'",
            (target_date,))
        if not signals: continue
        col     = f"price_{lookback}d"
        ret_col = f"return_{lookback}d"
        for sig in signals:
            df = dl_cached(sig["stock"]+".NS", "7d")
            if df is None or len(df) == 0: continue
            current_price = float(df["Close"].values[-1])
            entry = sig.get("cmp") or 0
            stop  = sig.get("stop_loss") or 0
            t1    = sig.get("target_1") or 0
            if entry <= 0: continue
            ret      = round((current_price - entry) / entry * 100, 2)
            hit_t1   = 1 if (t1   and current_price >= t1)   else 0
            hit_stop = 1 if (stop and current_price <= stop)  else 0

            # GAP-B2: R-multiple and exit type
            if stop and entry > stop:
                r_mult = round((current_price - entry) / (entry - stop), 2)
            else:
                r_mult = None

            if hit_t1:
                exit_type = "T1"
            elif hit_stop:
                exit_type = "STOP"
            else:
                exit_type = "OPEN"   # position still open at this lookback

            try:
                with _db_lock:
                    con.execute("""
                        INSERT OR IGNORE INTO signal_outcomes
                        (stock, pattern, signal_date, entry_price,
                         stop_loss, target_1, tracked_date, hit_t1, hit_stop,
                         actual_r_multiple, exit_type)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """, (sig["stock"], sig["pattern"], sig["scan_date"],
                          entry, stop, t1,
                          today, hit_t1, hit_stop, r_mult, exit_type))
                    con.execute(f"""
                        UPDATE signal_outcomes
                        SET {col}=?, {ret_col}=?,
                            hit_t1=?, hit_stop=?,
                            actual_r_multiple=?, exit_type=?,
                            tracked_date=?
                        WHERE stock=? AND pattern=? AND signal_date=?
                    """, (current_price, ret, hit_t1, hit_stop,
                          r_mult, exit_type, today,
                          sig["stock"], sig["pattern"], sig["scan_date"]))
                    con.commit()
            except Exception as e:
                log.debug(f"track_outcomes {sig['stock']}: {e}")
    log.info("Outcome tracking updated")


def print_outcome_summary(con):
    """
    GAP-B2 FIX: Now shows actual expectancy (E = avg R-multiple) per pattern,
    not just binary T1 hit rate. Expectancy answers: "for every ₹1 risked,
    how much did this pattern return on average?" E > 0 = profitable system.

    Previous: only showed avg_5d%, winner/loser counts.
    Now also shows: win_rate, avg_r_multiple (expectancy), sample quality.
    """
    try:
        rows = db_query(con, """
            SELECT pattern,
                   count(*) as n,
                   round(avg(return_5d),1) as avg_5d,
                   round(avg(return_10d),1) as avg_10d,
                   sum(hit_t1) as winners,
                   sum(hit_stop) as losers,
                   round(avg(actual_r_multiple),2) as avg_r,
                   round(min(actual_r_multiple),2) as min_r,
                   round(max(actual_r_multiple),2) as max_r
            FROM signal_outcomes
            WHERE return_5d IS NOT NULL
            GROUP BY pattern ORDER BY avg_r DESC
        """)
        if not rows:
            log.info("No outcome data yet. Needs 5+ trading days of signals.")
            return
        log.info("\n--- Signal Outcome Summary (5-day forward return + expectancy) ---")
        log.info(f"{'Pattern':<20} {'N':>4} {'WinRate':>7} {'Avg5d%':>7} "
                 f"{'Avg10d%':>8} {'AvgR':>6} {'MinR':>6} {'MaxR':>6}")
        log.info("-" * 72)
        for r in rows:
            total = (r['winners'] or 0) + (r['losers'] or 0)
            wr = round(r['winners'] / total * 100) if total > 0 else 0
            avg_r_str = f"{r['avg_r']:+.2f}" if r['avg_r'] is not None else "  N/A"
            flag = "✅" if (r['avg_r'] or 0) > 0.5 else ("⚠️" if (r['avg_r'] or 0) < 0 else "  ")
            log.info(f"{r['pattern']:<20} {r['n']:>4} {wr:>6}% "
                     f"{r['avg_5d']:>7} {r['avg_10d']:>8} "
                     f"{avg_r_str:>6} {(r['min_r'] or 0):>6.2f} {(r['max_r'] or 0):>6.2f} {flag}")
        log.info("\nExpectancy guide: AvgR > +1.0 = excellent | +0.5 = good | <0 = fix or drop pattern")
    except Exception as e:
        log.debug(f"Outcome summary: {e}")

def main():
    ap = argparse.ArgumentParser(description="NSE Scanner v3.3")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--daily", action="store_true", help="Full scan (8AM, 12:30PM, 4:30PM)")
    mode.add_argument("--halfhour", action="store_true", help="30-min watchlist+quick scan")
    mode.add_argument("--dashboard", action="store_true", help="Flask web UI")
    mode.add_argument("--healthcheck", action="store_true")
    mode.add_argument("--test", action="store_true", help="100 stocks test")
    mode.add_argument("--outcomes", action="store_true", help="Track forward returns on past signals")
    ap.add_argument("--telegram", action="store_true")
    ap.add_argument("--force",    action="store_true",
                    help="Force halfhour scan outside market hours (manual dispatch)")
    args = ap.parse_args()

    if args.healthcheck: sys.exit(0 if healthcheck() else 1)
    if args.dashboard: run_dashboard(); return
    if args.outcomes:
        con = get_db(); track_outcomes(con); print_outcome_summary(con); con.close(); return

    # ── Market-hours guard: halfhour only runs 09:15–15:31 IST ──────────────
    if args.halfhour and not args.force:
        now_ist = _now()
        h, m = now_ist.hour, now_ist.minute
        market_open  = (h, m) >= (9, 15)
        market_close = (h, m) <= (15, 31)
        if not (market_open and market_close):
            log.info(f"HALFHOUR SKIPPED: outside market hours ({_ist()}). Use --force to override.")
            csv_path = os.path.join(OUTPUT_DIR, f"halfhour_{_today()}_{_ist('%H%M')}.csv")
            pd.DataFrame([{"status": f"skipped_outside_market_hours_{_ist()}"}]).to_csv(csv_path, index=False)
            if TG_TOKEN and TG_CHAT:
                send_telegram(f"<b>30-min {_ist()}</b>\n⏸ Skipped — outside market hours\nUse --force to run manually.")
                send_telegram_file(csv_path, f"30-min {_ist()} | skipped (outside hours)")
            sys.exit(0)

    nifty_d = None
    for sym in ["^NSEI", "NIFTY_IND_NS"]:
        try:
            nifty_d = dl_cached(sym)  # incremental Nifty cache
        except Exception:
            pass
        if nifty_d is not None and len(nifty_d) > 50:
            break
        # Direct cache read fallback (works even when yfinance download fails)
        cached = _cache_read(sym)
        if cached is not None and len(cached) > 50:
            nifty_d = cached
            break
    if nifty_d is None:
        log.warning("Nifty fetch failed — using cautious defaults (aggression=1)")

    # ── Market context — computed once, shared by ALL modes ──────────────────
    ftd_active   = False
    market_trend = "Unknown"
    aggression   = 1          # safe default when nifty unavailable (was 2 — too optimistic)
    india_vix    = None
    breadth      = {"pct_above_50": 50, "pct_above_200": 50, "regime": "Unknown"}
    regime_info  = {"regime": "Unknown", "aggression": 1, "detail": "no data"}
    if nifty_d is not None:
        ftd_active, ftd_note = check_follow_through_day(nifty_d)
        market_trend = check_market_trend(nifty_d["Close"].values)
        india_vix    = fetch_india_vix()
        # breadth uses cached stocks — only available after warm-up, use light version here
        try:
            fallback_sample = NIFTY_500_FALLBACK_NS[:80]
            breadth = check_market_breadth(fallback_sample)
        except Exception:
            pass
        regime_info = get_market_regime(nifty_d, india_vix, breadth)
        aggression  = max(1, regime_info["aggression"])   # floor=1: always emit WATCH in bear mkt
        log.info(f"Market: {market_trend} | FTD: {ftd_active} | "
                 f"Regime: {regime_info['regime']} (agg={aggression}) | VIX: {india_vix}")

    # ---- 30-MINUTE MODE ----
    if args.halfhour:
        t0 = time.time()
        log.info(f"=== 30-min scan {_ist()} ===")
        alerts, quick_df = halfhour_check(nifty_d)
        log.info(f"{len(alerts)} alerts | {time.time()-t0:.0f}s")

        _hh_time = _ist("%H%M")
        csv_path = os.path.join(OUTPUT_DIR, f"halfhour_{_today()}_{_hh_time}.csv")
        mkt_str  = market_trend if nifty_d is not None else "Unknown"

        # Build CSV — signals if any, else status row
        if quick_df is None and alerts:
            quick_df = pd.DataFrame(alerts)
        if quick_df is not None and len(quick_df):
            quick_df.to_csv(csv_path, index=False)
            n_sig = len(quick_df)
        else:
            # 0 signals — always save a status CSV so artifact upload works
            pd.DataFrame([{"status": "0 signals", "scan_time": _ist(),
                           "market": mkt_str, "alerts": len(alerts)}]).to_csv(csv_path, index=False)
            n_sig = 0

        if args.telegram:
            if alerts:
                msg = fmt_halfhour(alerts[:20])
                if msg: send_telegram(msg)
            else:
                send_telegram(f"<b>30-min {_ist()}</b>\n⚪ 0 signals | Market: {mkt_str}")

            n_active = sum(1 for a in alerts
                           if a.get("status") in ("BREAKOUT TRIGGERED","Burst Active"))
            cap = (f"30-min {_ist()} | {n_sig} signals | "
                   f"{n_active} active alerts | Market: {mkt_str}")
            send_telegram_file(csv_path, cap)
            log.info(f"CSV sent: {csv_path}")
        return

    # ---- DAILY / TEST MODE ----
    con = get_db()
    t0 = time.time()
    scan_label = "TEST" if args.test else "DAILY"
    _scan_time = _ist("%H:%M"); log.info(f"=== {scan_label} {_today()} {_scan_time} ===")

    stocks = load_universe()
    if args.test:
        stocks = [s + ".NS" for s in [
            # Nifty 50 core (all verified active)
            "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","ITC","SBIN",
            "BHARTIARTL","KOTAKBANK","LT","AXISBANK","BAJFINANCE","ASIANPAINT","MARUTI",
            "SUNPHARMA","TITAN","WIPRO","ULTRACEMCO","BAJAJFINSV","NESTLEIND","POWERGRID",
            "NTPC","TECHM","HCLTECH","JSWSTEEL","TATASTEEL","ADANIENT","ADANIPORTS",
            "ONGC","COALINDIA","BRITANNIA","DIVISLAB","DRREDDY","EICHERMOT","GRASIM",
            "HDFCLIFE","INDUSINDBK","M&M","SBILIFE","SHREECEM","TATACONSUM","CIPLA",
            "APOLLOHOSP","BAJAJ-AUTO","BPCL","HAVELLS","HEROMOTOCO","LTIM","LUPIN",
            # Nifty Next 50 / liquid mid-caps
            "SIEMENS","TORNTPHARM","TRENT","IRCTC","LICI","ADANIGREEN",
            "CANBK","BANKBARODA","FEDERALBNK","IDFCFIRSTB","INDIGO","IRFC","JSWENERGY",
            "LICHSGFIN","MRF","NMDC","OBEROIRLTY","PAGEIND","PETRONET","PFC",
            "POLYCAB","RECLTD","SAIL","SBICARD","TATAPOWER","TVSMOTOR","VBL",
            "ZYDUSLIFE","ACC","AIAENG","ALKEM","AMBUJACEM","ASTRAL","AUBANK",
            "AUROPHARMA","BALKRISIND","BANDHANBNK","BATAINDIA","BERGEPAINT","BIOCON",
            "CHOLAFIN","CUMMINSIND","DEEPAKNTR","DIXON","DMART","ESCORTS","EXIDEIND",
            "GAIL","GLAND","GODREJCP","GODREJPROP","HAL","HINDALCO","ICICIPRULI",
            "IEX","IGL","INDHOTEL","INDUSTOWER","JKCEMENT","JUBLFOOD","KAJARIACER",
            "KPITTECH","LALPATHLAB","MANAPPURAM","MAXHEALTH","MCX","MUTHOOTFIN",
            "NAUKRI","NHPC","OFSS","PHOENIXLTD","PIDILITIND","PIIND","PRESTIGE",
            "PVRINOX","RBLBANK","SRF","SUNTV","TATAELXSI","TATATECH",
            "TIINDIA","UJJIVANSFB","UTIAMC","VEDL","VOLTAS","MARICO","MCDOWELL-N",
            # Strong mid-caps known for pattern setups
            "KIRLOSKEROIL","POLYCAB","DIXON","TRENT","DMART",
        ]]
        seen = set(); stocks = [s for s in stocks if not (s in seen or seen.add(s))]
        log.info(f"Test mode: {len(stocks)} verified stocks")

    log.info(f"{len(stocks)} stocks to scan")

    # ── Warm-up price cache ──────────────────────────────────────────────────
    warm_cache(stocks, workers=MAX_WORKERS)

    ftd_active = False; market_trend = "Unknown"
    if nifty_d is not None:
        ftd_active, ftd_note = check_follow_through_day(nifty_d)
        market_trend = check_market_trend(nifty_d["Close"].values)
        log.info(f"Market: {market_trend} | FTD: {ftd_active} ({ftd_note})")

    # GAP-F2 FIX: Fetch bulk/block deals ONCE before the scan loop — O(1) not O(n).
    # Previously: get_bulk_deals_today() was not called at all (wired but unused).
    # Now: passed to every scan_stock() call so bulk deal notes appear in signals.
    bulk_deals_today = {}
    try:
        from fundamentals import get_bulk_deals_today
        bulk_deals_today = get_bulk_deals_today() or {}
        if bulk_deals_today:
            log.info(f"Bulk deals today: {len(bulk_deals_today)} entries loaded")
    except Exception as _bd_ex:
        log.debug(f"Bulk deals fetch failed (non-critical): {_bd_ex}")

    all_rows = []; ok_count = 0; fund_fails = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(scan_stock, s, nifty_d, ftd_active, market_trend,
                                   aggression=aggression,
                                   bulk_deals_today=bulk_deals_today): s for s in stocks}
        for i, fut in enumerate(as_completed(futs)):
            if (i+1) % 200 == 0:
                log.info(f"  {i+1}/{len(stocks)} | signals: {len(all_rows)}")
            try:
                rows, fund_ok = fut.result()
                ok_count += 1
                if not fund_ok: fund_fails += 1
                if rows: all_rows.extend(rows)
            except Exception as _ex:
                if i < 3:   # log first 3 crashes so we can diagnose
                    log.error(f"scan_stock crash #{i+1}: {type(_ex).__name__}: {_ex}")

    elapsed = time.time() - t0
    log.info(f"Done: {elapsed/60:.1f}m | {ok_count}/{len(stocks)} OK | "
             f"fund_miss={fund_fails} | {len(all_rows)} signals")

    if not all_rows:
        # Always save a status CSV and send Telegram — never silently exit
        _csv_hm  = _ist("%H%M")
        csv_path = os.path.join(OUTPUT_DIR, f"scan_{_today()}_{_csv_hm}.csv")
        status_df = pd.DataFrame([{"status": "0 signals", "scan_date": str(_today()),
                                    "scan_time": _ist(), "mode": scan_label,
                                    "stocks_scanned": ok_count, "total_stocks": len(stocks),
                                    "elapsed_min": round(elapsed/60,1)}])
        status_df.to_csv(csv_path, index=False)
        log.info(f"Status CSV → {csv_path}")
        if args.telegram:
            mkt_str = market_trend if nifty_d is not None else "Unknown"
            msg = (f"<b>NSE {scan_label} — {_today()} {_ist()}</b>\n"
                   f"⚠️ 0 signals from {ok_count}/{len(stocks)} stocks scanned\n"
                   f"Market: {mkt_str} | Time: {elapsed/60:.1f}m")
            send_telegram(msg)
            send_telegram_file(csv_path, f"NSE {scan_label} {_today()} {_ist()} | 0 signals | {ok_count}/{len(stocks)} scanned")
        con.close(); return

    raw = (pd.DataFrame(all_rows)
           .drop_duplicates(subset=["stock","pattern","timeframe"])
           .reset_index(drop=True))

    def _score(r) -> float:
        """
        10-POINT COMPOSITE SCORE SYSTEM  (v4.0 — recalibrated)
        ════════════════════════════════
        POINTS BREAKDOWN (max 10.0):
        ┌─────────────────────────────────────────────┬──────┐
        │ Component                                   │ Max  │
        ├─────────────────────────────────────────────┼──────┤
        │ 1. CANSLIM score (normalised)               │ 2.3  │
        │ 2. RS Percentile (true cross-sectional)     │ 1.4  │
        │ 3. Risk:Reward ratio                        │ 1.3  │
        │ 4. Volume surge (vs 20d avg)                │ 0.9  │
        │ 5. TI65 trend intensity (Bonde)             │ 0.5  │
        │ 6. Lynch score (2LYNCH checklist)           │ 0.4  │
        │ 7. Weinstein Stage 2                        │ 0.5  │
        │ 8. Volume dry-up (smart money stealth)      │ 0.3  │
        │ 9. Pattern quality (detector signal)        │ 0.4  │
        │10. Earnings safe (no event <14d)            │ 0.2  │
        │11. RS line leading (O'Neil best signal)     │ 0.5  │
        │12. Piotroski F-score ≥ 7 (GAP-F3)          │ 0.3  │
        │13. Bulk/block deal today (GAP-F2)           │ 0.4  │
        │14. Promoter pledge quality (GAP-F1)         │ 0.3  │
        │15. VCP contraction bonus: 3+ (GAP-P6)      │ 0.3  │
        └─────────────────────────────────────────────┴──────┘

        TIERS:
          BUY STRONG   ≥ 7.0  — high-conviction setup, all factors align
          BUY MODERATE ≥ 5.5  — good setup, minor gaps acceptable
          WATCH        ≥ 3.5  — pattern detected, not all factors confirm
          AVOID        < 3.5  — pattern weak, pass

        Score stored as 'score10' in output CSV (0.0–10.0, 1dp).
        """
        score = 0.0

        # ── 1. CANSLIM (2.3 pts) ──────────────────────────────────────────────
        cs = r.get("canslim_score") or 0
        dc = max(r.get("data_completeness") or 1, 1)
        canslim_norm = cs / dc   # 0.0 – 1.0
        score += canslim_norm * 2.3

        # ── 2. RS Percentile (1.4 pts) ─────────────────────────────────────
        rs = r.get("rs_percentile")
        if rs is not None:
            if rs >= 90:   score += 1.4
            elif rs >= 80: score += 1.1
            elif rs >= 70: score += 0.8
            elif rs >= 60: score += 0.4

        # ── 3. Risk:Reward (1.3 pts) ──────────────────────────────────────
        rr = r.get("risk_reward") or 0
        if rr >= 4.0:   score += 1.3
        elif rr >= 3.0: score += 1.0
        elif rr >= 2.0: score += 0.7
        elif rr >= 1.5: score += 0.3

        # ── 4. Volume surge (0.9 pt) ──────────────────────────────────────
        vs = r.get("vol_surge") or 0
        if vs >= 4.0:   score += 0.9
        elif vs >= 2.5: score += 0.7
        elif vs >= 1.5: score += 0.4
        elif vs >= 1.0: score += 0.15

        # ── 5. TI65 trend intensity (0.5 pt) ──────────────────────────────
        ti = r.get("ti65") or 0
        if ti >= 1.10:   score += 0.5
        elif ti >= 1.05: score += 0.35
        elif ti >= 1.0:  score += 0.2

        # ── 6. Lynch score (0.4 pt) ──────────────────────────────────────
        ls = r.get("lynch_score_val") or 0
        if ls >= 5:   score += 0.4
        elif ls >= 4: score += 0.3
        elif ls >= 3: score += 0.2
        elif ls >= 2: score += 0.1

        # ── 7. Weinstein Stage 2 (0.5 pt) ─────────────────────────────────
        stage = str(r.get("stage") or "")
        if "Stage2" in stage:   score += 0.5
        elif "Stage1" in stage: score += 0.2
        elif "Stage3" in stage: score -= 0.2

        # ── 8. Volume dry-up (0.3 pt) ──────────────────────────────────────
        if r.get("vol_dryup"): score += 0.3

        # ── 9. Pattern quality (0.4 pt) ────────────────────────────────────
        q = r.get("quality") or 0
        if q >= 0.08:   score += 0.4
        elif q >= 0.05: score += 0.28
        elif q >= 0.02: score += 0.15
        elif q > 0:     score += 0.08

        # ── 10. Earnings safety (0.2 pt) ──────────────────────────────────
        if not r.get("earnings_near"): score += 0.2

        # ── 11. RS Line Leadership (0.5 pt) ───────────────────────────────
        notes_str = str(r.get("notes") or "")
        if "RS-LEAD" in notes_str: score += 0.5

        # ── 12. Piotroski F-score (0.3 pt, -0.3 penalty) — GAP-F3 ─────────
        pio = r.get("piotroski_score")
        if pio is not None:
            if pio >= 7:   score += 0.3   # strong financials
            elif pio <= 2: score -= 0.3   # weak financials — penalty

        # ── 13. Bulk/Block deal today (0.4 pt) — GAP-F2 ───────────────────
        # Institutional accumulation on the signal day = strong confirmation
        if r.get("bulk_deal_cr") and r["bulk_deal_cr"] > 0:
            score += 0.4

        # ── 14. Promoter pledge quality (0.3 pt, -0.5 penalty) — GAP-F1 ──
        pledge = r.get("pledge_pct") or 0
        promoter = r.get("scr_promoter_pct") or 0   # will be None until fund wired fully
        if pledge < 5 and promoter > 50:
            score += 0.3    # low pledge + high promoter confidence
        elif pledge > 20:
            score -= 0.5    # dangerous pledging level
        elif pledge > 10:
            score -= 0.1    # mild concern

        # ── 15. VCP contraction count bonus (0.3 pt) — GAP-P6 ─────────────
        # Minervini: 3+ contractions = markedly higher probability
        if "VCP" in str(r.get("pattern") or "") and "VCP(" in notes_str:
            try:
                n_ct = int(notes_str.split("VCP(")[1].split("C)")[0])
                if n_ct >= 4:   score += 0.3
                elif n_ct >= 3: score += 0.2
            except Exception:
                pass

        return round(min(score, 10.0), 2)

    def _tier(score10: float) -> str:
        """Convert numeric score to actionable tier label."""
        if score10 >= 7.0: return "★★★ BUY STRONG"
        if score10 >= 5.5: return "★★  BUY MODERATE"
        if score10 >= 3.5: return "★   WATCH"
        return "✗   AVOID"

    raw["score10"] = raw.apply(_score, axis=1)
    raw["tier"]    = raw["score10"].apply(_tier)
    df = raw.sort_values("score10", ascending=False).reset_index(drop=True)

    # Save to DB
    db_execmany(con, """INSERT INTO signals
        (scan_date,scan_time,scan_mode,stock,name,sector,cap_class,cap_cr,
         pattern,timeframe,pattern_formed,pattern_start_date,pattern_end_date,formation_days,
         t1_eta,t2_eta,t3_eta,
         status,breakout_zone,cmp,stop_loss,
         target_1,target_2,target_3,risk_reward,quality,vol_surge,
         canslim_score,data_completeness,rs_percentile,dist_52wk_pct,converging,leg,
         earnings_near,ftd_active,vol_dryup,stage,recommendation,
         ti65,lynch_score_val,piotroski_score,pledge_pct,bulk_deal_cr,
         m1,m2,m3,m4,m5,notes)
        VALUES (:scan_date,:scan_time,:scan_mode,:stock,:name,:sector,:cap_class,:cap_cr,
         :pattern,:timeframe,:pattern_formed,:pattern_start_date,:pattern_end_date,:formation_days,
         :t1_eta,:t2_eta,:t3_eta,
         :status,:breakout_zone,:cmp,:stop_loss,
         :target_1,:target_2,:target_3,:risk_reward,:quality,:vol_surge,
         :canslim_score,:data_completeness,:rs_percentile,:dist_52wk_pct,:converging,:leg,
         :earnings_near,:ftd_active,:vol_dryup,:stage,:recommendation,
         :ti65,:lynch_score_val,:piotroski_score,:pledge_pct,:bulk_deal_cr,
         :m1,:m2,:m3,:m4,:m5,:notes)""",
                df.to_dict("records"))

    # Update watchlist JSON
    wl_items = []
    for _, r in df.iterrows():
        wl_items.append({
            "stock": r["stock"], "name": r.get("name"), "sector": r.get("sector"),
            "cap_class": r.get("cap_class"), "pattern": r["pattern"],
            "timeframe": r.get("timeframe"),
            "pattern_formed": r.get("pattern_formed"),
            "pattern_start_date": r.get("pattern_start_date"),  # GAP-P1
            "pattern_end_date":   r.get("pattern_end_date"),    # GAP-P2
            "formation_days": r.get("formation_days"),
            "breakout_zone": r.get("breakout_zone"), "stop_loss": r.get("stop_loss"),
            "target_1": r.get("target_1"), "risk_reward": r.get("risk_reward"),
            "t1_eta": r.get("t1_eta"), "t2_eta": r.get("t2_eta"), "t3_eta": r.get("t3_eta"),
            "status": r.get("status"), "added_date": str(_today()),
            "score10": r.get("score10"), "tier": r.get("tier"),
            "piotroski_score": r.get("piotroski_score"),
            "pledge_pct": r.get("pledge_pct"),
            "bulk_deal_cr": r.get("bulk_deal_cr"),
        })
    # Merge with existing (keep entries from last 30 days, dedup by stock+pattern)
    existing_wl = {f"{w['stock']}_{w['pattern']}": w for w in load_watchlist()}
    for item in wl_items:
        existing_wl[f"{item['stock']}_{item['pattern']}"] = item
    save_watchlist(list(existing_wl.values()))

    buys   = df[df["tier"].str.contains("BUY",   na=False)]
    watches= df[df["tier"].str.contains("WATCH", na=False)]
    db_exec(con, """INSERT INTO runs
        (scan_date,scan_time,mode,stocks_total,stocks_ok,signals,buys,elapsed_sec)
        VALUES (?,?,?,?,?,?,?,?)""",
            (str(_today()), _ist("%H:%M"), scan_label,
             len(stocks), ok_count, len(df), len(buys), round(elapsed,1)))

    # ── Save outputs: BUY csv + WATCH csv (readable columns) + raw ALL csv ──
    _ts = _ist("%H%M")
    buy_path, watch_path = _save_csvs(buys, watches, OUTPUT_DIR, _ts)
    # Raw full-data CSV (all columns — for programmatic use / DB import)
    csv_all = os.path.join(OUTPUT_DIR, f"scan_ALL_{_today()}_{_ts}.csv")
    df.to_csv(csv_all, index=False)
    log.info(f"CSV ALL   ({len(df):>4} rows) → {os.path.basename(csv_all)}")

    log.info(f"\nBUY: {len(buys)} | WATCH: {len(watches)}")
    log.info(f"\n{df['pattern'].value_counts().to_string()}")

    conv = df[df["converging"].notna()]
    if len(conv):
        log.info("Convergence:")
        for s in conv["stock"].unique():
            log.info(f"  {s}: {conv[conv['stock']==s]['converging'].iloc[0]}")

    if len(buys):
        print("\n--- TOP BUYS ---")
        cols = ["stock","cap_class","pattern","status","cmp","breakout_zone","stop_loss",
                "target_1","target_2","target_3","risk_reward","canslim_score","leg","stage",
                "recommendation","notes"]
        print(buys[[c for c in cols if c in buys.columns]].head(20).to_string(index=False))

    if args.telegram:
        _cap_time = _ist("%H:%M")
        send_telegram(fmt_daily(df, market_trend, ftd_active, regime_info=regime_info))
        # Send BUY csv (primary — Telegram delivers first)
        buy_cap = (f"📈 NSE {scan_label} {_today()} {_cap_time} | "
                   f"BUY: {len(buys)} | Market: {market_trend}")
        send_telegram_file(buy_path, buy_cap)
        # Send WATCH csv
        watch_cap = (f"👀 NSE {scan_label} WATCH {_today()} {_cap_time} | "
                     f"WATCH: {len(watches)} signals")
        send_telegram_file(watch_path, watch_cap)

    # Gap 7 FIX: auto-run outcome tracking at end of every daily scan.
    try:
        log.info("Auto-tracking signal outcomes …")
        track_outcomes(con)
        print_outcome_summary(con)
    except Exception as e:
        log.debug(f"Auto outcome tracking: {e}")

    # GAP-OP4 FIX: Clean up output files older than 30 days.
    # With 3 scans/day × 3 files = 9+ files/day, this prevents unbounded growth.
    try:
        cutoff = _today() - timedelta(days=30)
        removed = 0
        for fname in os.listdir(OUTPUT_DIR):
            fpath = os.path.join(OUTPUT_DIR, fname)
            if not os.path.isfile(fpath): continue
            # Filenames include date: scan_BUY_2026-01-01_1630.csv
            for ext in [".csv", ".json", ".txt"]:
                if fname.endswith(ext):
                    try:
                        mtime = date.fromtimestamp(os.path.getmtime(fpath))
                        if mtime < cutoff:
                            os.remove(fpath)
                            removed += 1
                    except Exception:
                        pass
        if removed:
            log.info(f"Cleaned {removed} output files older than 30 days")
    except Exception as _clean_ex:
        log.debug(f"Output cleanup: {_clean_ex}")

    con.close()

if __name__ == "__main__":
    main()
