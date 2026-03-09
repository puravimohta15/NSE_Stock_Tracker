import yfinance as yf
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from scripts.indicators import compute_indicators

def chunk_list(lst, size):
    """Split ticker list into chunks"""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def convert_to_weekly(df):
    weekly = df.resample("W").agg({
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last",
        "Volume": "sum"
    })
    return weekly.dropna()

def process_ticker(ticker, data, market_caps):

    try:
        if isinstance(data.columns, pd.MultiIndex):
            if ticker not in data.columns.levels[0]:
                return None
            df = data[ticker].copy()
        else:
            df = data.copy()

        if df.empty:
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        if isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index).tz_localize(None)

        df = convert_to_weekly(df)

        if len(df) < 50:
            return None

        df = compute_indicators(df)

        latest = df.iloc[-1]
        prev = df.iloc[-2]

        # -------------------------
        # BUY CONDITIONS
        # -------------------------

        ema_condition = latest["EMA10"] > latest["EMA20"]

        price_above_ema50 = latest["Close"] > latest["EMA50"]

        rsi_range = 35 <= latest["RSI"] <= 55
        rsi_turn_up = latest["RSI"] > prev["RSI"]

        price_lower_low = latest["Low"] <= prev["Low"]
        rsi_higher_low = latest["RSI"] > prev["RSI"]
        bullish_divergence = price_lower_low and rsi_higher_low

        avg_vol = df["Volume"].iloc[-5:-1].mean()
        volume_condition = latest["Volume"] > avg_vol

        near_high = (latest["High"] - latest["Close"]) / latest["High"] <= 0.02

        market_cap = market_caps.loc[ticker]["MarketCap"]
        market_cap_condition = market_cap > 5e10

        buy_signal = (
            ema_condition
            and price_above_ema50
            and rsi_range
            and rsi_turn_up
            and bullish_divergence
            and volume_condition
            and market_cap_condition
            and near_high
        )

        # -------------------------
        # EXIT CONDITIONS
        # -------------------------

        drop_from_high = (latest["High"] - latest["Close"]) / latest["High"] > 0.10

        rsi_exit = latest["RSI"] < 50

        ema_exit = latest["EMA10"] < latest["EMA20"]

        ema50_exit = latest["Close"] < latest["EMA50"]

        price_higher_high = latest["High"] > prev["High"]
        rsi_lower_high = latest["RSI"] < prev["RSI"]
        bearish_divergence = price_higher_high and rsi_lower_high

        exit_signal = (
            drop_from_high
            or rsi_exit
            or ema_exit
            or ema50_exit
            or bearish_divergence
        )

        if buy_signal:
            return {
                "Stock": ticker,
                "Signal": "BUY",
                "Price": round(float(latest["Close"]), 2),
                "RSI": round(float(latest["RSI"]), 2),
            }

        if exit_signal:
            return {
                "Stock": ticker,
                "Signal": "EXIT",
                "Price": round(float(latest["Close"]), 2),
                "RSI": round(float(latest["RSI"]), 2),
            }

    except Exception as e:
        print(f"Error processing {ticker}: {e}")

    return None


def scan_stocks(tickers):
    results = []
    print("Scanning stocks...")

    batch_size = 100
    market_caps = pd.read_csv("data/market_caps.csv", index_col="Stock")
    for batch in chunk_list(tickers, batch_size):
        print(f"Processing batch of {len(batch)} tickers")

        # Batch download all tickers in the batch
        try:
            data = yf.download(
                batch,
                period="1y",
                group_by="ticker",
                threads=True,
                progress=False
            )
        except Exception as e:
            print(f"Batch download failed: {e}")
            continue

        # Parallel processing for each ticker
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(process_ticker, ticker, data, market_caps) for ticker in batch]

            for future in as_completed(futures):
                result = future.result()
                if result:
                    results.append(result)

    return pd.DataFrame(results)