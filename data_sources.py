#!/usr/bin/env python3
"""
NSE Scanner v3.7 — Modular Data Sources Layer
==============================================

Abstracts data fetching with priority-based fallbacks:
  Primary: nsepy (official NSE), Screener.in (fundamentals)
  Fallback: yfinance (if primary sources fail)
  
This module implements Gaps 1, 3, 5 fixes using free/official sources.

Usage:
    from data_sources import dl_ohlcv, dl_fundamentals, get_delivery_pct
    
    # OHLCV data (Gap 1)
    df = dl_ohlcv("RELIANCE")  # tries nsepy first, falls back to yfinance
    
    # Fundamentals (Gap 3)
    fund = dl_fundamentals("RELIANCE")
    print(fund['eps'], fund['sales_growth'], fund['roe'])
    
    # NSE-specific (Gap 5)
    delivery = get_delivery_pct("RELIANCE", "2025-01-24")
    fo_data = get_fo_oi_data("RELIANCE")
    fii_flow = get_fii_dii_flow("2025-01-24")
"""

import pandas as pd
import numpy as np
import requests
import yfinance as yf
import logging
from datetime import date, timedelta
from io import StringIO
from typing import Optional

log = logging.getLogger(__name__)

# ========================
# GAP 1: OHLCV Data Source
# ========================

def dl_ohlcv(sym: str, start_date: Optional[date] = None, 
             end_date: Optional[date] = None, source: str = "auto") -> Optional[pd.DataFrame]:
    """
    Fetch OHLCV data with priority fallback chain.
    
    Priority:
      1. nsepy (official NSE) — most reliable
      2. yfinance (Yahoo) — fallback
    
    Args:
        sym: Stock symbol, format "RELIANCE" or "RELIANCE.NS"
        start_date: Start date (default: 1 year ago)
        end_date: End date (default: today)
        source: "auto", "nsepy", "yfinance"
    
    Returns:
        DataFrame with columns: Open, High, Low, Close, Volume, Adj Close
    """
    if start_date is None:
        start_date = date.today() - timedelta(days=365)
    if end_date is None:
        end_date = date.today()
    
    clean_sym = sym.replace(".NS", "")
    
    # Try nsepy first (official NSE)
    if source in ("auto", "nsepy"):
        df = _dl_nsepy(clean_sym, start_date, end_date)
        if df is not None and len(df) >= 20:
            log.info(f"[Gap 1] Fetched {clean_sym} via nsepy ({len(df)} rows)")
            return df
    
    # Fall back to yfinance
    if source in ("auto", "yfinance"):
        df = _dl_yfinance(sym, start_date, end_date)
        if df is not None and len(df) >= 20:
            log.info(f"[Gap 1] Fetched {clean_sym} via yfinance ({len(df)} rows) [fallback]")
            return df
    
    log.warning(f"[Gap 1] Failed to fetch {clean_sym} from any source")
    return None


def _dl_nsepy(sym: str, start_date: date, end_date: date) -> Optional[pd.DataFrame]:
    """
    Fetch directly from NSE via nsepy library (official source).
    
    Gap 1 Fix: Removes dependency on Yahoo's aggregated data.
    Handles corporate actions (splits, bonuses) automatically.
    """
    try:
        from nsepy import get_history
        
        df = get_history(symbol=sym, start=start_date, end=end_date, 
                        series="EQ", index_date=True)
        
        if df is None or len(df) == 0:
            return None
        
        # Standardize columns to match yfinance
        # nsepy returns: Open, High, Low, Close, Volume, series
        df = df[['Open', 'High', 'Low', 'Close', 'Volume']].copy()
        df['Adj Close'] = df['Close']  # NSE data is already adjusted
        df = df.astype(float)
        df.index = pd.to_datetime(df.index)
        
        return df
    except ImportError:
        log.debug("[Gap 1] nsepy not installed (pip install nsepy)")
        return None
    except Exception as e:
        log.debug(f"[Gap 1] nsepy fetch failed for {sym}: {e}")
        return None


def _dl_yfinance(sym: str, start_date: date, end_date: date) -> Optional[pd.DataFrame]:
    """
    Fallback: Fetch from yfinance (Yahoo).
    """
    try:
        # Ensure .NS suffix for NSE stocks
        if not sym.endswith((".NS", ".BO")):
            sym = f"{sym}.NS"
        
        df = yf.download(sym, start=start_date, end=end_date, 
                        auto_adjust=True, progress=False, timeout=20)
        
        if df is None or len(df) == 0:
            return None
        
        # yfinance returns: Open, High, Low, Close, Adj Close, Volume
        df = df[['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']].copy()
        df = df.astype(float)
        return df
    except Exception as e:
        log.debug(f"[Gap 1] yfinance fetch failed for {sym}: {e}")
        return None


# ===========================
# GAP 3: Fundamentals (CANSLIM)
# ===========================

def dl_fundamentals(sym: str, source: str = "auto") -> dict:
    """
    Fetch quarterly fundamentals (EPS, sales growth, P/E, ROE, etc).
    
    Priority:
      1. Screener.in (free aggregated data, institutional-grade)
      2. yfinance (fallback, less reliable)
    
    Args:
        sym: Stock symbol "RELIANCE" or "RELIANCE.NS"
        source: "auto", "screener", "yfinance"
    
    Returns:
        Dict with keys:
        - _fund_ok: bool (data was fetched successfully)
        - marketCap: float (market capitalization in crores)
        - eps: float (earnings per share, TTM or latest quarter)
        - eps_growth: float (% growth YoY)
        - sales: float (annual sales in crores)
        - sales_growth: float (% growth YoY) — [Gap 3]
        - net_profit_margin: float (%)
        - roe: float (Return on Equity, %) — [Gap 3]
        - roa: float (Return on Assets, %)
        - pe_ratio: float
        - pb_ratio: float
        - debt_to_equity: float
        - promoter_holding: float (%)
        - institutional_holding: float (%)
        - sector: str
    
    Gap 3 Fix: Provides actual sales growth (not just price-based metrics).
    """
    clean_sym = sym.replace(".NS", "")
    
    # Try Screener.in first
    if source in ("auto", "screener"):
        fund = _dl_screener(clean_sym)
        if fund.get("_fund_ok"):
            log.info(f"[Gap 3] Fetched {clean_sym} fundamentals via Screener.in")
            return fund
    
    # Fall back to yfinance
    if source in ("auto", "yfinance"):
        fund = _dl_yfinance_fundamentals(sym)
        if fund.get("_fund_ok"):
            log.info(f"[Gap 3] Fetched {clean_sym} fundamentals via yfinance [fallback]")
            return fund
    
    log.warning(f"[Gap 3] Failed to fetch fundamentals for {clean_sym}")
    return {"_fund_ok": False}


def _dl_screener(sym: str) -> dict:
    """
    Fetch fundamentals from Screener.in API (free, no auth required).
    
    Gap 3 Fix: Screener.in aggregates quarterly filings from NSE,
    providing actual sales and profit data (not Yahoo's estimates).
    """
    try:
        url = f"https://api.screener.in/v1/company/{sym}/info/"
        
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Referer': 'https://www.screener.in',
            'Accept': 'application/json',
        })
        
        resp = session.get(url, timeout=10)
        if resp.status_code != 200:
            log.debug(f"[Gap 3] Screener.in {sym} HTTP {resp.status_code}")
            return {"_fund_ok": False}
        
        data = resp.json()
        cmp = data.get('company', {})
        
        return {
            "_fund_ok": True,
            "marketCap": cmp.get('market_cap'),
            "eps": cmp.get('eps'),
            "eps_growth": cmp.get('eps_growth'),
            "sales": cmp.get('sales'),
            "sales_growth": cmp.get('sales_growth'),  # [Gap 3] real sales growth
            "net_profit_margin": cmp.get('net_profit_margin'),
            "roe": cmp.get('roe'),  # [Gap 3] actual ROE from filings
            "roa": cmp.get('roa'),
            "pe_ratio": cmp.get('pe'),
            "pb_ratio": cmp.get('pb'),
            "debt_to_equity": cmp.get('debt_to_equity'),
            "promoter_holding": cmp.get('promoter_holding'),
            "institutional_holding": cmp.get('institutional_holding'),
            "sector": cmp.get('sector'),
            "industry": cmp.get('industry'),
            "latest_quarter": cmp.get('q'),  # e.g., "Q3 FY2025"
        }
    except Exception as e:
        log.debug(f"[Gap 3] Screener.in fetch failed for {sym}: {e}")
        return {"_fund_ok": False}


def _dl_yfinance_fundamentals(sym: str) -> dict:
    """
    Fallback: Fetch fundamentals from yfinance.
    Less reliable than Screener.in but works as a safety net.
    """
    try:
        if not sym.endswith((".NS", ".BO")):
            sym = f"{sym}.NS"
        
        ticker = yf.Ticker(sym)
        info = ticker.info
        
        return {
            "_fund_ok": True,
            "marketCap": info.get('marketCap'),
            "eps": info.get('trailingEps'),
            "eps_growth": info.get('epsTrailingTwelveMonths'),
            "sales": info.get('totalRevenue'),
            "sales_growth": info.get('revenueGrowth'),
            "net_profit_margin": info.get('profitMargins'),
            "roe": info.get('returnOnEquity'),
            "roa": info.get('returnOnAssets'),
            "pe_ratio": info.get('trailingPE'),
            "pb_ratio": info.get('priceToBook'),
            "debt_to_equity": info.get('debtToEquity'),
            "sector": info.get('sector'),
            "industry": info.get('industry'),
        }
    except Exception as e:
        log.debug(f"[Gap 3] yfinance fundamentals failed for {sym}: {e}")
        return {"_fund_ok": False}


# ====================================
# GAP 5: NSE-Specific Data
# ====================================

def get_delivery_pct(sym: str, date_str: str) -> Optional[float]:
    """
    Gap 5A: Fetch delivery percentage from NSE Bhavcopy CSV.
    
    NSE publishes daily Bhavcopy CSVs containing:
    - DELIVERYCLNT: deliverable quantity
    - TOTTRDQTY: total traded quantity
    - Delivery % = DELIVERYCLNT / TOTTRDQTY
    
    High delivery % (>60%) = institutional accumulation
    Low delivery % (<30%) = intra-day trading/speculation
    
    Args:
        sym: Stock symbol "RELIANCE"
        date_str: Date string "2025-01-24" or "24JAN2025"
    
    Returns:
        float (0-100) or None if data not available
    """
    try:
        clean_sym = sym.replace(".NS", "")
        
        # Parse date to both formats
        date_obj = pd.to_datetime(date_str)
        formatted_date = date_obj.strftime("%d%b%Y").upper()
        formatted_year = date_obj.strftime("%Y")
        formatted_month = date_obj.strftime("%b").upper()
        
        # NSE Bhavcopy URL: eq_ddmmmyyyy.csv in YYYY/MMM folder
        url = (f"https://www.nseindia.com/content/historical/EQUITIES/"
               f"{formatted_year}/{formatted_month}/eq_{formatted_date.lower()}.csv")
        
        session = requests.Session()
        session.headers.update({'User-Agent': 'Mozilla/5.0'})
        
        resp = session.get(url, timeout=10)
        if resp.status_code != 200:
            log.debug(f"[Gap 5A] Bhavcopy {formatted_date} HTTP {resp.status_code}")
            return None
        
        df = pd.read_csv(StringIO(resp.text))
        
        # Find the symbol row
        row = df[df['SYMBOL'].str.strip() == clean_sym]
        if row.empty:
            log.debug(f"[Gap 5A] {clean_sym} not found in Bhavcopy {formatted_date}")
            return None
        
        traded_qty = float(row['TOTTRDQTY'].values[0])
        delivery_qty = float(row['DELIVERYCLNT'].values[0])
        
        if traded_qty == 0:
            return None
        
        delivery_pct = (delivery_qty / traded_qty) * 100
        log.info(f"[Gap 5A] {clean_sym} delivery: {delivery_pct:.2f}% on {formatted_date}")
        return round(delivery_pct, 2)
    except Exception as e:
        log.debug(f"[Gap 5A] Delivery % failed for {sym} {date_str}: {e}")
        return None


def get_fo_oi_data(sym: str, expiry: str = "current") -> Optional[dict]:
    """
    Gap 5B: Fetch F&O open interest and Put-Call Ratio from NSE.
    
    NSE publishes option chain data via their API, including:
    - Call OI and Put OI by strike price
    - PCR (Put-Call Ratio): put OI / call OI
      - PCR > 1.5 = fear (over-selling)
      - PCR < 0.7 = complacency (over-buying)
    - Max Pain = strike price where option sellers lose least (max hedging)
    
    Args:
        sym: Stock symbol "RELIANCE"
        expiry: "current" (nearest expiry) or date like "26-DEC-2024"
    
    Returns:
        Dict with keys: expiry, call_oi, put_oi, pcr, total_oi, max_pain
        or None if data not available
    """
    try:
        clean_sym = sym.replace(".NS", "").upper()
        
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Referer': 'https://www.nseindia.com',
        })
        
        # Get available expiries
        expiry_url = "https://www.nseindia.com/api/option/expirydates"
        expiry_resp = session.get(expiry_url, timeout=10)
        if expiry_resp.status_code != 200:
            return None
        
        expiries = expiry_resp.json().get('data', [])
        if not expiries:
            return None
        
        # Use specified or nearest expiry
        if expiry == "current":
            expiry = expiries[0]
        
        # Fetch option chain for this symbol and expiry
        chain_url = f"https://www.nseindia.com/api/option-chain-equities?symbol={clean_sym}&expiry_date={expiry}"
        chain_resp = session.get(chain_url, timeout=10)
        if chain_resp.status_code != 200:
            log.debug(f"[Gap 5B] Option chain {clean_sym} HTTP {chain_resp.status_code}")
            return None
        
        chain_data = chain_resp.json()
        records = chain_data.get('records', {})
        data_list = records.get('data', [])
        
        # Aggregate all call and put OI
        call_oi = sum(float(r.get('CE', {}).get('openInterest', 0)) 
                     for r in data_list)
        put_oi = sum(float(r.get('PE', {}).get('openInterest', 0)) 
                    for r in data_list)
        
        total_oi = call_oi + put_oi
        pcr = (put_oi / call_oi) if call_oi > 0 else None
        
        max_pain = records.get('maxPain')
        
        result = {
            "expiry": expiry,
            "call_oi": int(call_oi),
            "put_oi": int(put_oi),
            "total_oi": int(total_oi),
            "pcr": round(pcr, 2) if pcr else None,
            "max_pain": max_pain,
        }
        
        log.info(f"[Gap 5B] {clean_sym} {expiry}: PCR {result['pcr']}, Max Pain {max_pain}")
        return result
    except Exception as e:
        log.debug(f"[Gap 5B] F&O OI failed for {sym}: {e}")
        return None


def get_fii_dii_flow(date_str: str = None) -> Optional[dict]:
    """
    Gap 5C: Fetch daily FII/DII flow data from NSE.
    
    NSE publishes daily institutional flows:
    - FII: Foreign Institutional Investor (typically long-term, stable)
    - DII: Domestic Institutional Investor (insurance, mutual funds, pension)
    
    High positive flow = accumulation phase
    High negative flow = distribution phase
    
    Args:
        date_str: Date "2025-01-24" (default: today)
    
    Returns:
        Dict with keys: date, fii_net, fii_buy, fii_sell, dii_net, dii_buy, dii_sell
        or None if data not available
    """
    try:
        if date_str is None:
            date_str = str(date.today())
        
        date_obj = pd.to_datetime(date_str)
        nsdate = date_obj.strftime("%d-%b-%Y")
        
        # NSE publishes FII/DII data via this endpoint
        url = f"https://www.nseindia.com/api/fii_fiidii_equity/{nsdate}"
        
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Referer': 'https://www.nseindia.com',
        })
        
        resp = session.get(url, timeout=10)
        if resp.status_code != 200:
            log.debug(f"[Gap 5C] FII/DII {nsdate} HTTP {resp.status_code}")
            return None
        
        data = resp.json()
        
        result = {
            "date": nsdate,
            "fii_net": float(data.get('FII_net_buy_sell', 0)),
            "fii_buy": float(data.get('FII_Gross_Buy', 0)),
            "fii_sell": float(data.get('FII_Gross_Sell', 0)),
            "dii_net": float(data.get('DII_net_buy_sell', 0)),
            "dii_buy": float(data.get('DII_Gross_Buy', 0)),
            "dii_sell": float(data.get('DII_Gross_Sell', 0)),
            "net_flow": float(data.get('FII_net_buy_sell', 0)) + float(data.get('DII_net_buy_sell', 0)),
        }
        
        log.info(f"[Gap 5C] {nsdate}: FII {result['fii_net']:,.0f}cr, DII {result['dii_net']:,.0f}cr")
        return result
    except Exception as e:
        log.debug(f"[Gap 5C] FII/DII fetch failed for {date_str}: {e}")
        return None


def get_bulk_deals(date_str: str = None, sym: str = None) -> Optional[list]:
    """
    Gap 5D: Fetch bulk deals and block deals from NSE.
    
    NSE publishes trades of >0.5% shares (bulk deals) and ≥₹10 Cr (block deals).
    These are the clearest institutional accumulation/distribution signals.
    
    A bulk deal by promoters = positive signal
    A bulk deal by FIIs = watchful (could be profit booking)
    
    Args:
        date_str: Date "2025-01-24" (default: today)
        sym: Filter by symbol (optional)
    
    Returns:
        List of dicts with keys: Symbol, Quantity, Price, ClientName, BuySell, Date
        or None if data not available
    """
    try:
        if date_str is None:
            date_str = str(date.today())
        
        date_obj = pd.to_datetime(date_str)
        nsdate = date_obj.strftime("%d-%b-%Y")
        
        # NSE bulk deals CSV
        url = "https://www.nseindia.com/cgi-bin/bulkdealdownload.cgi"
        
        session = requests.Session()
        session.headers.update({'User-Agent': 'Mozilla/5.0'})
        
        # NSE bulk deals endpoint returns CSV
        resp = session.get(url, timeout=10, params={"date": nsdate})
        if resp.status_code != 200:
            log.debug(f"[Gap 5D] Bulk deals {nsdate} HTTP {resp.status_code}")
            return None
        
        bulk_df = pd.read_csv(StringIO(resp.text))
        
        # Filter for today (if multiple days in CSV)
        bulk_df['Date'] = pd.to_datetime(bulk_df['Date'], format='%d-%b-%Y')
        today_deals = bulk_df[bulk_df['Date'] == date_obj.date()]
        
        # Filter by symbol if provided
        if sym:
            clean_sym = sym.replace(".NS", "")
            today_deals = today_deals[today_deals['Symbol'] == clean_sym]
        
        if len(today_deals) == 0:
            log.debug(f"[Gap 5D] No bulk deals on {nsdate}" + (f" for {sym}" if sym else ""))
            return None
        
        deals = today_deals.to_dict('records')
        log.info(f"[Gap 5D] Found {len(deals)} bulk deal(s) on {nsdate}")
        return deals
    except Exception as e:
        log.debug(f"[Gap 5D] Bulk deals fetch failed: {e}")
        return None


# ====================================
# Convenience: Fetch All (Scanner Integration)
# ====================================

def fetch_all_data(sym: str, date_str: str = None) -> dict:
    """
    Fetch all available data for a symbol (Gaps 1, 3, 5).
    
    Returns a comprehensive dict suitable for passing to scanner.
    """
    if date_str is None:
        date_str = str(date.today())
    
    return {
        # Gap 1: OHLCV
        "ohlcv": dl_ohlcv(sym),
        
        # Gap 3: Fundamentals
        "fundamentals": dl_fundamentals(sym),
        
        # Gap 5: NSE-specific
        "delivery_pct": get_delivery_pct(sym, date_str),
        "fo_oi": get_fo_oi_data(sym),
        "fii_dii": get_fii_dii_flow(date_str),
        "bulk_deals": get_bulk_deals(date_str, sym),
    }
