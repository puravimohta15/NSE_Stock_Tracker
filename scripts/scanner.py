import yfinance as yf
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from scripts.indicators import compute_indicators
from db.cache import MarketCache

# initialize cache
cache = MarketCache()


def chunk_list(lst, size):
    """Split ticker list into chunks"""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def process_ticker(ticker, data):
    """Process a single ticker (runs in parallel)"""

    try:

        # 1. Extract ticker data from batch download
        if isinstance(data.columns, pd.MultiIndex):
            if ticker not in data.columns.levels[0]:
                return None
            df_download = data[ticker].copy()
        else:
            df_download = data.copy()

        if df_download.empty:
            return None

        # Flatten columns if needed
        if isinstance(df_download.columns, pd.MultiIndex):
            df_download.columns = df_download.columns.get_level_values(0)

        # Ensure tz-naive datetime index
        if isinstance(df_download.index, pd.DatetimeIndex):
            df_download.index = pd.to_datetime(df_download.index).tz_localize(None)

        # 2. Load cached data
        cached_df = cache.get_data(ticker)
        last_date = cache.get_last_date(ticker)

        df_new = None

        if last_date is None:
            df_new = df_download
        else:
            start = (pd.to_datetime(last_date) + pd.Timedelta(days=1)).normalize()
            df_new = df_download[df_download.index >= start]

        got_new = False

        if df_new is not None and not df_new.empty:
            cache.upsert_data(ticker, df_new)
            got_new = True

        # 3. Finalize dataframe
        if got_new:
            df_full = cache.get_data(ticker)
        else:
            df_full = cached_df

        if df_full is None or len(df_full) < 50:
            return None


        # 4. Compute indicators
        df_full = compute_indicators(df_full)

        latest = df_full.iloc[-1]
        prev = df_full.iloc[-2]

        l_ema10, l_ema20 = float(latest["EMA10"]), float(latest["EMA20"])
        p_ema10, p_ema20 = float(prev["EMA10"]), float(prev["EMA20"])

        bullish = (p_ema10 < p_ema20) and (l_ema10 > l_ema20)
        bearish = (p_ema10 > p_ema20) and (l_ema10 < p_ema20)

        if bullish or bearish:
            return {
                "Stock": ticker,
                "Signal": "Bullish" if bullish else "Bearish",
                "Price": round(float(latest["Close"]), 2),
                "RSI": round(float(latest["RSI"]), 2)
            }

    except Exception as e:
        print(f"Error processing {ticker}: {e}")

    return None


def scan_stocks(tickers):
    results = []
    print("Scanning stocks (using local cache)...")

    batch_size = 100

    for batch in chunk_list(tickers, batch_size):

        print(f"Processing batch of {len(batch)} tickers")
        
        try:
            data = yf.download(
                batch,
                period="6mo",
                group_by="ticker",
                threads=True,
                progress=False
            )
        except Exception as e:
            print(f"Batch download failed: {e}")
            continue

        with ThreadPoolExecutor(max_workers=8) as executor:

            futures = [
                executor.submit(process_ticker, ticker, data)
                for ticker in batch
            ]

            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    results.append(result)

    return pd.DataFrame(results)