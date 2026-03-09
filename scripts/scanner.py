import yfinance as yf
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from scripts.indicators import compute_indicators

def chunk_list(lst, size):
    """Split ticker list into chunks"""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def process_ticker(ticker, data):
    """Process a single ticker (runs in parallel)"""
    try:
        # Extract ticker data from batch download
        if isinstance(data.columns, pd.MultiIndex):
            if ticker not in data.columns.levels[0]:
                return None
            df = data[ticker].copy()
        else:
            df = data.copy()

        if df.empty:
            return None

        # Flatten MultiIndex if needed
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # Ensure tz-naive datetime index
        if isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index).tz_localize(None)

        # Keep only the last 6 months (just to be safe)
        df = df.tail(120)

        # Skip if not enough data
        if len(df) < 50:
            return None

        # Compute indicators
        df = compute_indicators(df)

        latest = df.iloc[-1]
        prev = df.iloc[-2]

        l_ema10, l_ema20 = float(latest["EMA10"]), float(latest["EMA20"])
        p_ema10, p_ema20 = float(prev["EMA10"]), float(prev["EMA20"])

        bullish = (p_ema10 < p_ema20) and (l_ema10 > l_ema20)
        bearish = (p_ema10 > p_ema20) and (l_ema10 < l_ema20)

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
    print("Scanning stocks...")

    batch_size = 100

    for batch in chunk_list(tickers, batch_size):
        print(f"Processing batch of {len(batch)} tickers")

        # Batch download all tickers in the batch
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

        # Parallel processing for each ticker
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(process_ticker, ticker, data) for ticker in batch]

            for future in as_completed(futures):
                result = future.result()
                if result:
                    results.append(result)

    return pd.DataFrame(results)