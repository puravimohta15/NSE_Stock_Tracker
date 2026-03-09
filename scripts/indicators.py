import pandas as pd
from concurrent.futures import ProcessPoolExecutor


def compute_indicators(df):

    df = df.copy()

    # EMAs
    df["EMA10"] = df["Close"].ewm(span=10, adjust=False).mean()
    df["EMA20"] = df["Close"].ewm(span=20, adjust=False).mean()
    df["EMA50"] = df["Close"].ewm(span=50, adjust=False).mean()

    # RSI
    delta = df["Close"].diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()

    rs = avg_gain / avg_loss

    df["RSI"] = 100 - (100 / (1 + rs))

    return df


def compute_indicators_parallel(data_dict, max_workers=6):
    """
    data_dict format:
    {
        "RELIANCE.NS": dataframe,
        "TCS.NS": dataframe
    }
    """

    results = {}

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(compute_indicators, df): ticker
            for ticker, df in data_dict.items()
        }

        for future in futures:
            ticker = futures[future]
            try:
                results[ticker] = future.result()
            except:
                continue

    return results