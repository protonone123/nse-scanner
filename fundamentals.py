#!/usr/bin/env python3
"""
fundamentals.py — Multi-source fundamental data fetcher for NSE stocks
=======================================================================
Sources:
  1. yfinance    — PE, EPS growth, revenue, institutions, float, debt/equity
  2. Screener.in — promoter holding, pledging, Piotroski, ROCE, ROE, 3yr growth
  3. NSE bulk    — daily bulk/block deals (official NSE CSV, free)
  4. NSE results — earnings dates from NSE announcements

All results cached in fund_cache + bulk_deals tables (shared price_cache.db).
Scanner imports: get_fundamentals(sym), get_bulk_deals_today(), get_screener_data(sym)
"""

import os, json, time, logging, sqlite3, re
from datetime import date, datetime, timedelta, timezone
from threading import Lock
from io import StringIO

log = logging.getLogger("fundamentals")

_IST = timezone(timedelta(hours=5, minutes=30))
def _today(): return datetime.now(_IST).date()

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
CACHE_PATH = os.path.join(BASE_DIR, "price_cache.db")

_db_lock = Lock()
_db_con  = None

# ================================================================
# DB CONNECTION (shared with data_updater + scanner)
# ================================================================
def _get_db():
    global _db_con
    with _db_lock:
        if _db_con is None:
            _db_con = sqlite3.connect(CACHE_PATH, check_same_thread=False)
            _db_con.execute("PRAGMA journal_mode=WAL")
            _db_con.executescript("""
                CREATE TABLE IF NOT EXISTS fund_cache (
                    stock        TEXT PRIMARY KEY,
                    fund_json    TEXT,
                    updated_date TEXT
                );
                CREATE TABLE IF NOT EXISTS bulk_deals (
                    deal_date   TEXT NOT NULL,
                    stock       TEXT NOT NULL,
                    client_name TEXT,
                    deal_type   TEXT,
                    qty         REAL,
                    price       REAL,
                    exchange    TEXT,
                    PRIMARY KEY (deal_date, stock, client_name)
                );
                CREATE TABLE IF NOT EXISTS screener_cache (
                    stock        TEXT PRIMARY KEY,
                    data_json    TEXT,
                    updated_date TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_bulk_date  ON bulk_deals(deal_date);
                CREATE INDEX IF NOT EXISTS idx_bulk_stock ON bulk_deals(stock);
            """)
            _db_con.commit()
        return _db_con


# ================================================================
# YFINANCE ENHANCED FUNDAMENTALS
# ================================================================
def _fetch_yfinance(sym: str) -> dict:
    """
    Full yfinance fundamentals for one stock.
    Returns structured dict with all available metrics.
    """
    result = {"_source": "yfinance", "_ok": False}
    try:
        import yfinance as yf
        try:
            from curl_cffi import requests as _cr
            sess = _cr.Session(impersonate="chrome110")
            sess.get("https://finance.yahoo.com", timeout=10)
        except Exception:
            sess = None

        kw = {"session": sess} if sess else {}
        tk = yf.Ticker(sym, **kw)
        info = tk.info or {}
        if not info.get("regularMarketPrice") and not info.get("currentPrice"):
            return result

        result.update({
            "_ok": True,
            # Valuation
            "pe_ratio":             info.get("trailingPE"),
            "forward_pe":           info.get("forwardPE"),
            "pb_ratio":             info.get("priceToBook"),
            "ps_ratio":             info.get("priceToSalesTrailing12Months"),
            "ev_ebitda":            info.get("enterpriseToEbitda"),
            "peg_ratio":            info.get("pegRatio"),
            # Size
            "market_cap":           info.get("marketCap"),
            "enterprise_value":     info.get("enterpriseValue"),
            # Growth
            "eps_growth_qoq":       info.get("earningsQuarterlyGrowth"),
            "eps_growth_yoy":       info.get("earningsGrowth"),
            "revenue_growth_yoy":   info.get("revenueGrowth"),
            # Profitability
            "roe":                  info.get("returnOnEquity"),
            "roa":                  info.get("returnOnAssets"),
            "profit_margin":        info.get("profitMargins"),
            "gross_margin":         info.get("grossMargins"),
            "operating_margin":     info.get("operatingMargins"),
            # Financial health
            "debt_to_equity":       info.get("debtToEquity"),
            "current_ratio":        info.get("currentRatio"),
            "quick_ratio":          info.get("quickRatio"),
            "free_cashflow":        info.get("freeCashflow"),
            "operating_cashflow":   info.get("operatingCashflow"),
            # Ownership
            "inst_holding_pct":     info.get("heldPercentInstitutions"),
            "insider_holding_pct":  info.get("heldPercentInsiders"),
            "float_shares":         info.get("floatShares"),
            "shares_outstanding":   info.get("sharesOutstanding"),
            "shares_short_pct":     info.get("shortPercentOfFloat"),
            # Dividends
            "dividend_yield":       info.get("dividendYield"),
            "payout_ratio":         info.get("payoutRatio"),
            # Per-share
            "book_value_per_share": info.get("bookValue"),
            "eps_ttm":              info.get("trailingEps"),
            "eps_forward":          info.get("forwardEps"),
            # Price reference
            "52wk_high":            info.get("fiftyTwoWeekHigh"),
            "52wk_low":             info.get("fiftyTwoWeekLow"),
            "avg_vol_10d":          info.get("averageVolume10days"),
            "avg_vol_3mo":          info.get("averageVolume"),
            # Company info
            "sector":               info.get("sector"),
            "industry":             info.get("industry"),
            "long_name":            info.get("longName") or info.get("shortName"),
            "employees":            info.get("fullTimeEmployees"),
            "country":              info.get("country"),
        })

        # Computed fields
        mc = result.get("market_cap") or 0
        if mc:
            result["cap_cr"] = round(mc / 1e7, 1)
            result["cap_class"] = (
                "Large" if mc >= 2e12 else
                "Mid"   if mc >= 5e11 else
                "Small" if mc >= 5e10 else "Micro"
            )
        fl = result.get("float_shares") or 0
        so = result.get("shares_outstanding") or 0
        if so > 0 and fl > 0:
            result["free_float_pct"] = round(fl / so * 100, 1)

        # Earnings calendar
        try:
            cal = tk.calendar
            ne = cal.get("Earnings Date", [None])[0] if cal else None
            result["next_earnings"] = str(ne)[:10] if ne else None
        except Exception:
            result["next_earnings"] = None

        # Income statement — 4Q EPS for acceleration check
        try:
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                qf = tk.quarterly_financials
            if qf is not None and len(qf.columns) >= 4:
                net_inc = qf.loc["Net Income"] if "Net Income" in qf.index else None
                if net_inc is not None:
                    eps_vals = net_inc.values[:4]  # last 4 quarters
                    result["eps_q1"] = float(eps_vals[0]) if len(eps_vals) > 0 else None
                    result["eps_q2"] = float(eps_vals[1]) if len(eps_vals) > 1 else None
                    result["eps_q3"] = float(eps_vals[2]) if len(eps_vals) > 2 else None
                    result["eps_q4"] = float(eps_vals[3]) if len(eps_vals) > 3 else None
                    # EPS acceleration check (3+ quarters of increasing growth)
                    if all(v is not None for v in [result.get("eps_q1"), result.get("eps_q2"),
                                                    result.get("eps_q3"), result.get("eps_q4")]):
                        q_vals = [result["eps_q4"], result["eps_q3"], result["eps_q2"], result["eps_q1"]]
                        growth_rates = []
                        for i in range(1, len(q_vals)):
                            if q_vals[i-1] != 0:
                                growth_rates.append((q_vals[i] - q_vals[i-1]) / abs(q_vals[i-1]))
                        result["eps_accelerating"] = (
                            len(growth_rates) >= 3 and
                            all(growth_rates[i] > growth_rates[i-1]
                                for i in range(1, len(growth_rates)))
                        )
        except Exception:
            pass

    except Exception as e:
        log.debug(f"yfinance {sym}: {e}")

    return result


# ================================================================
# SCREENER.IN SCRAPER
# ================================================================
def _fetch_screener(sym_clean: str) -> dict:
    """
    Scrape Screener.in for NSE stock fundamentals.
    Returns promoter holding, pledging, Piotroski, ROCE, ROE, growth.
    sym_clean: stock symbol without .NS (e.g. 'RELIANCE')
    """
    result = {"_source": "screener", "_ok": False}
    try:
        import requests as req
        from bs4 import BeautifulSoup

        url = f"https://www.screener.in/company/{sym_clean}/consolidated/"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        resp = req.get(url, headers=headers, timeout=15)
        if resp.status_code == 404:
            # Try standalone
            url2 = f"https://www.screener.in/company/{sym_clean}/"
            resp = req.get(url2, headers=headers, timeout=15)
        if resp.status_code != 200:
            return result

        soup = BeautifulSoup(resp.text, "html.parser")

        def _get_ratio(label: str) -> float | None:
            """Find a ratio value by its label in the page."""
            for li in soup.find_all("li", class_="flex"):
                span = li.find("span", class_="name")
                val  = li.find("span", class_="number")
                if span and val and label.lower() in span.get_text(strip=True).lower():
                    try:
                        txt = val.get_text(strip=True).replace(",","").replace("%","")
                        return float(txt)
                    except Exception:
                        return None
            return None

        # Key ratios
        result["scr_roe"]              = _get_ratio("ROE")
        result["scr_roce"]             = _get_ratio("ROCE")
        result["scr_debt_equity"]      = _get_ratio("Debt / Equity")
        result["scr_pe"]               = _get_ratio("Stock P/E")
        result["scr_pb"]               = _get_ratio("Price to Book")
        result["scr_dividend_yield"]   = _get_ratio("Dividend Yield")
        result["scr_current_ratio"]    = _get_ratio("Current ratio")

        # Promoter + pledging from shareholding section
        shp_section = soup.find("section", {"id": "shareholding"})
        if shp_section:
            tables = shp_section.find_all("table")
            for tbl in tables:
                rows = tbl.find_all("tr")
                for row in rows:
                    cells = row.find_all("td")
                    if not cells: continue
                    label = cells[0].get_text(strip=True).lower()
                    if "promoter" in label and "pledged" not in label and len(cells) >= 2:
                        try:
                            result["scr_promoter_pct"] = float(
                                cells[-1].get_text(strip=True).replace("%",""))
                        except Exception: pass
                    if "pledg" in label and len(cells) >= 2:
                        try:
                            result["scr_pledging_pct"] = float(
                                cells[-1].get_text(strip=True).replace("%",""))
                        except Exception: pass

        # 10-year compounded growth rates
        growth_section = soup.find("section", {"id": "profit-loss"})
        if growth_section:
            comp_rows = growth_section.find_all("tr")
            for row in comp_rows:
                txt = row.get_text(" ", strip=True).lower()
                if "compounded sales growth" in txt or "compounded revenue" in txt:
                    cells = row.find_all("td")
                    for c in cells:
                        t = c.get_text(strip=True)
                        if "3 years" in t.lower() and len(cells) > 1:
                            try:
                                idx = cells.index(c)
                                result["scr_revenue_growth_3yr"] = float(
                                    cells[idx+1].get_text(strip=True).replace("%",""))
                            except Exception: pass
                if "compounded profit growth" in txt:
                    cells = row.find_all("td")
                    for c in cells:
                        t = c.get_text(strip=True)
                        if "3 years" in t.lower() and len(cells) > 1:
                            try:
                                idx = cells.index(c)
                                result["scr_profit_growth_3yr"] = float(
                                    cells[idx+1].get_text(strip=True).replace("%",""))
                            except Exception: pass

        # Piotroski score (if shown)
        for tag in soup.find_all(string=re.compile("Piotroski", re.I)):
            parent = tag.parent
            sib = parent.find_next_sibling()
            if sib:
                try:
                    result["scr_piotroski"] = int(sib.get_text(strip=True))
                except Exception: pass

        result["_ok"] = any(v is not None for k, v in result.items()
                            if k.startswith("scr_"))

    except ImportError:
        log.debug("bs4 not installed — Screener.in scraping disabled. pip install beautifulsoup4")
    except Exception as e:
        log.debug(f"Screener.in {sym_clean}: {e}")

    return result


# ================================================================
# NSE BULK DEALS
# ================================================================
def fetch_nse_bulk_deals(target_date: date | None = None) -> list[dict]:
    """
    Download NSE bulk deals CSV for a given date.
    Official source: https://nsearchives.nseindia.com/content/equities/bulk.csv
    Returns list of deal dicts.
    """
    if target_date is None:
        target_date = _today()

    deals = []
    urls = [
        "https://nsearchives.nseindia.com/content/equities/bulk.csv",
        f"https://www.nseindia.com/api/historical/bulk-deals?from={target_date}&to={target_date}",
    ]

    try:
        import requests as req
        headers = {
            "User-Agent":  "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept":      "text/html,application/xhtml+xml,*/*;q=0.8",
            "Referer":     "https://www.nseindia.com/",
        }

        # Try CSV first
        resp = req.get(urls[0], headers=headers, timeout=20)
        if resp.status_code == 200:
            import io, csv
            reader = csv.DictReader(io.StringIO(resp.text))
            for row in reader:
                try:
                    deal_date_str = row.get("Date", "") or row.get("TRADE_DATE", "")
                    # Parse various date formats
                    for fmt in ["%d-%b-%Y", "%d/%m/%Y", "%Y-%m-%d"]:
                        try:
                            deal_date = datetime.strptime(deal_date_str.strip(), fmt).date()
                            break
                        except Exception:
                            deal_date = None
                    if deal_date != target_date:
                        continue
                    symbol = (row.get("Symbol","") or row.get("SYMBOL","")).strip().upper()
                    if not symbol:
                        continue
                    deals.append({
                        "date":        str(deal_date),
                        "stock":       symbol,
                        "client":      (row.get("Client Name","") or row.get("CLIENT_NAME","")).strip(),
                        "deal_type":   (row.get("Buy/Sell","") or row.get("BUY_SELL","")).strip(),
                        "qty":         _safe_float(row.get("Quantity Traded","") or row.get("QTY","")),
                        "price":       _safe_float(row.get("Trade Price","") or row.get("PRICE","")),
                        "exchange":    "NSE",
                    })
                except Exception:
                    continue

        # If CSV empty, try JSON API
        if not deals:
            session = req.Session()
            # Get cookies first
            session.get("https://www.nseindia.com", headers=headers, timeout=10)
            api_resp = session.get(
                f"https://www.nseindia.com/api/historical/bulk-deals?"
                f"from={target_date.strftime('%d-%m-%Y')}&to={target_date.strftime('%d-%m-%Y')}",
                headers={**headers, "Accept": "application/json"},
                timeout=15
            )
            if api_resp.status_code == 200:
                data = api_resp.json()
                for row in (data.get("data") or []):
                    deals.append({
                        "date":      str(target_date),
                        "stock":     row.get("symbol","").strip().upper(),
                        "client":    row.get("clientName","").strip(),
                        "deal_type": row.get("buySell","").strip(),
                        "qty":       _safe_float(row.get("quantityTraded","")),
                        "price":     _safe_float(row.get("tradePrice","")),
                        "exchange":  "NSE",
                    })

    except Exception as e:
        log.warning(f"NSE bulk deals fetch failed: {e}")

    # Store in DB
    if deals:
        _store_bulk_deals(deals)

    log.info(f"NSE bulk deals {target_date}: {len(deals)} deals")
    return deals


def _safe_float(val) -> float | None:
    try:
        return float(str(val).replace(",","").strip())
    except Exception:
        return None


def _store_bulk_deals(deals: list[dict]):
    try:
        con = _get_db()
        with _db_lock:
            con.executemany(
                "INSERT OR REPLACE INTO bulk_deals "
                "(deal_date,stock,client_name,deal_type,qty,price,exchange) "
                "VALUES (:date,:stock,:client,:deal_type,:qty,:price,:exchange)",
                deals
            )
            con.commit()
    except Exception as e:
        log.debug(f"Store bulk deals: {e}")


def get_bulk_deals_today() -> dict[str, list[dict]]:
    """
    Returns dict mapping stock_symbol → list of deals today.
    Scanner uses this to flag insider/institutional activity.
    """
    try:
        con = _get_db()
        today = str(_today())
        rows = con.execute(
            "SELECT stock,client_name,deal_type,qty,price "
            "FROM bulk_deals WHERE deal_date=?",
            (today,)
        ).fetchall()
        result: dict[str, list] = {}
        for r in rows:
            sym = r[0]
            if sym not in result:
                result[sym] = []
            result[sym].append({
                "client": r[1], "type": r[2],
                "qty": r[3], "price": r[4],
            })
        return result
    except Exception:
        return {}


def get_bulk_deal_value(stock: str) -> tuple[float | None, str | None]:
    """
    Returns (deal_value_cr, deal_type) for today's largest deal on a stock.
    """
    deals = get_bulk_deals_today()
    stock_deals = deals.get(stock.replace(".NS",""), [])
    if not stock_deals:
        return None, None
    # Find largest deal by value
    best = max(stock_deals,
               key=lambda d: (d.get("qty") or 0) * (d.get("price") or 0))
    val_cr = ((best.get("qty") or 0) * (best.get("price") or 0)) / 1e7
    return round(val_cr, 2) if val_cr > 0 else None, best.get("type")


# ================================================================
# NSE ANNOUNCEMENTS — earnings dates
# ================================================================
def fetch_nse_earnings_dates() -> dict[str, str]:
    """
    Fetch upcoming board meetings / results from NSE corporate actions.
    Returns dict: {SYMBOL: date_str}
    """
    result = {}
    try:
        import requests as req
        headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.nseindia.com/"}
        session = req.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        resp = session.get(
            "https://www.nseindia.com/api/event-calendar",
            headers={**headers, "Accept": "application/json"},
            timeout=15
        )
        if resp.status_code == 200:
            data = resp.json()
            for item in (data if isinstance(data, list) else []):
                sym  = item.get("symbol","").strip().upper()
                dt   = item.get("date","")
                desc = item.get("purpose","").lower()
                if sym and dt and any(k in desc for k in ["result","board","financial"]):
                    result[sym] = dt[:10]
    except Exception as e:
        log.debug(f"NSE earnings dates: {e}")
    return result


# ================================================================
# PIOTROSKI F-SCORE — computed from yfinance data
# ================================================================
def calc_piotroski(info: dict) -> int | None:
    """
    Simplified Piotroski F-Score from available yfinance data.
    Max 6 points from available metrics (full score needs balance sheet).
    """
    score = 0
    checks = 0

    # Profitability
    if info.get("returnOnAssets") is not None:
        checks += 1
        if info["returnOnAssets"] > 0: score += 1

    if info.get("operatingCashflow") is not None:
        checks += 1
        if info["operatingCashflow"] > 0: score += 1

    if info.get("earningsGrowth") is not None:
        checks += 1
        if info["earningsGrowth"] > 0: score += 1

    # Leverage / Liquidity
    if info.get("currentRatio") is not None:
        checks += 1
        if info["currentRatio"] > 1.0: score += 1

    if info.get("debtToEquity") is not None:
        checks += 1
        if info["debtToEquity"] < 1.0: score += 1

    # Operating efficiency
    if info.get("grossMargins") is not None:
        checks += 1
        if info["grossMargins"] > 0.2: score += 1

    return score if checks >= 3 else None


# ================================================================
# MAIN FETCHER — combines all sources
# ================================================================
def get_fundamentals(sym: str, force_refresh: bool = False) -> dict:
    """
    Get full fundamentals for a stock. Uses cached data if available today.
    sym: with or without .NS suffix
    Returns merged dict from all sources.
    """
    sym_ns    = sym if sym.endswith(".NS") else sym + ".NS"
    sym_clean = sym.replace(".NS","").upper()

    # Check cache
    if not force_refresh:
        try:
            con = _get_db()
            row = con.execute(
                "SELECT fund_json, updated_date FROM fund_cache WHERE stock=?",
                (sym_ns,)
            ).fetchone()
            if row and row[0] and row[1] == str(_today()):
                return json.loads(row[0])
        except Exception:
            pass

    # Fetch from all sources
    result = {"stock": sym_clean, "_fetched": str(_today())}

    # Source 1: yfinance
    yf_data = _fetch_yfinance(sym_ns)
    result.update({k: v for k, v in yf_data.items() if not k.startswith("_")})
    result["_yf_ok"] = yf_data.get("_ok", False)

    # Source 2: Screener.in (only if yfinance succeeded — avoid double failures)
    if result["_yf_ok"]:
        try:
            scr = _fetch_screener(sym_clean)
            result.update({k: v for k, v in scr.items()
                          if not k.startswith("_") and v is not None})
            result["_scr_ok"] = scr.get("_ok", False)
        except Exception:
            result["_scr_ok"] = False
    else:
        result["_scr_ok"] = False

    # Computed: Piotroski from yfinance data
    if result["_yf_ok"]:
        result["piotroski_score"] = calc_piotroski(yf_data)

    # EPS acceleration check
    q_eps = [result.get(f"eps_q{i}") for i in range(1, 5)]
    if all(v is not None for v in q_eps):
        try:
            oldest_to_newest = list(reversed(q_eps))
            growths = []
            for i in range(1, len(oldest_to_newest)):
                if oldest_to_newest[i-1] and oldest_to_newest[i-1] != 0:
                    growths.append((oldest_to_newest[i] - oldest_to_newest[i-1])
                                   / abs(oldest_to_newest[i-1]))
            result["eps_accelerating"] = (
                len(growths) >= 2 and
                all(growths[i] > growths[i-1] for i in range(1, len(growths)))
            )
        except Exception:
            pass

    # Cache result
    try:
        con = _get_db()
        with _db_lock:
            con.execute(
                "INSERT OR REPLACE INTO fund_cache (stock,fund_json,updated_date) VALUES (?,?,?)",
                (sym_ns, json.dumps(result, default=str), str(_today()))
            )
            con.commit()
    except Exception:
        pass

    return result


def get_screener_cached(sym_clean: str) -> dict:
    """Get screener data with separate screener_cache table (refreshes weekly)."""
    try:
        con = _get_db()
        row = con.execute(
            "SELECT data_json, updated_date FROM screener_cache WHERE stock=?",
            (sym_clean,)
        ).fetchone()
        cutoff = str(_today() - timedelta(days=7))
        if row and row[0] and row[1] >= cutoff:
            return json.loads(row[0])
    except Exception:
        pass

    data = _fetch_screener(sym_clean)
    try:
        con = _get_db()
        with _db_lock:
            con.execute(
                "INSERT OR REPLACE INTO screener_cache (stock,data_json,updated_date) "
                "VALUES (?,?,?)",
                (sym_clean, json.dumps(data), str(_today()))
            )
            con.commit()
    except Exception:
        pass
    return data


# ================================================================
# SIGNAL QUALITY HELPERS — used by scanner
# ================================================================
def promoter_quality_score(fund: dict) -> float:
    """
    Score 0-1 based on promoter holding + pledging.
    High holding + low pledging = 1.0 (institutional confidence)
    Low holding + high pledging = 0.0 (danger zone)
    """
    # Try Screener.in data first, yfinance insider as fallback
    promoter_pct = fund.get("scr_promoter_pct") or (
        (fund.get("insider_holding_pct") or 0) * 100
    )
    pledging_pct = fund.get("scr_pledging_pct") or 0

    # Promoter holding score: 50%+ = good, below 30% = bad
    ph_score = min(max((promoter_pct - 30) / 40, 0), 1) if promoter_pct else 0.5

    # Pledging penalty: 0% = no penalty, 30%+ = max penalty
    pledge_penalty = min(pledging_pct / 30, 1) if pledging_pct else 0

    return round(ph_score * (1 - pledge_penalty * 0.5), 3)


def earnings_quality_score(fund: dict) -> float:
    """
    Score 0-1 based on earnings acceleration and growth quality.
    """
    score = 0.5  # neutral base

    eps_growth = fund.get("eps_growth_qoq") or fund.get("eps_growth_yoy") or 0
    rev_growth = fund.get("revenue_growth_yoy") or fund.get("scr_revenue_growth_3yr") or 0
    roe        = fund.get("roe") or fund.get("scr_roe") or 0
    if isinstance(roe, (int, float)) and roe > 1:
        roe = roe / 100  # normalize if in percent

    # Bonuses
    if eps_growth > 0.25:  score += 0.15
    if eps_growth > 0.50:  score += 0.10
    if rev_growth > 0.20:  score += 0.10
    if rev_growth > 0.40:  score += 0.10
    if roe > 0.20:         score += 0.10
    if fund.get("eps_accelerating"): score += 0.15
    p = fund.get("piotroski_score")
    if p is not None:
        if p >= 5: score += 0.10
        if p <= 2: score -= 0.20

    return round(min(max(score, 0), 1), 3)


def is_low_float(fund: dict) -> bool:
    """Qullamaggie's key filter: float < 20% of outstanding shares."""
    ff = fund.get("free_float_pct")
    if ff is not None:
        return ff < 20
    # Approximate from institutions
    inst = fund.get("inst_holding_pct") or 0
    insider = fund.get("insider_holding_pct") or 0
    return (inst + insider) > 0.80


def has_insider_activity(stock_clean: str, deals_today: dict) -> tuple[bool, float | None, str | None]:
    """
    Check if stock has bulk/block deal today.
    Returns (has_deal, deal_value_cr, deal_type)
    """
    deals = deals_today.get(stock_clean.upper(), [])
    if not deals:
        return False, None, None
    best = max(deals, key=lambda d: (d.get("qty") or 0) * (d.get("price") or 0))
    val = ((best.get("qty") or 0) * (best.get("price") or 0)) / 1e7
    return True, round(val, 2) if val > 0 else None, best.get("type")


# ================================================================
# BATCH REFRESH — called by data_updater
# ================================================================
def batch_refresh_fundamentals(stocks: list[str], workers: int = 4):
    """
    Refresh fundamentals for a list of stocks.
    Skips stocks already fetched today.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    today = str(_today())
    con   = _get_db()

    # Find which need refresh
    need_refresh = []
    for sym in stocks:
        sym_ns = sym if sym.endswith(".NS") else sym + ".NS"
        try:
            row = con.execute(
                "SELECT updated_date FROM fund_cache WHERE stock=?", (sym_ns,)
            ).fetchone()
            if not row or row[0] != today:
                need_refresh.append(sym_ns)
        except Exception:
            need_refresh.append(sym_ns)

    if not need_refresh:
        log.info("Fundamentals: all up to date")
        return

    log.info(f"Fundamentals: refreshing {len(need_refresh)}/{len(stocks)} stocks")
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(get_fundamentals, s): s for s in need_refresh}
        for fut in as_completed(futs):
            done += 1
            try:
                fut.result()
            except Exception:
                pass
            if done % 100 == 0:
                log.info(f"  Fundamentals: {done}/{len(need_refresh)}")
    log.info(f"Fundamentals refresh done: {len(need_refresh)} stocks")


if __name__ == "__main__":
    import sys, pprint
    sym = sys.argv[1] if len(sys.argv) > 1 else "RELIANCE.NS"
    print(f"\nFetching fundamentals for {sym}...")
    data = get_fundamentals(sym, force_refresh=True)
    pprint.pprint(data)
    print(f"\nBulk deals today: {get_bulk_deals_today()}")
