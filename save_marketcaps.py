from get_tickers import get_nse_tickers
import yfinance as yf
import pandas as pd
import time

if __name__ == "__main__":
    print("Loading tickers...")

    tickers = get_nse_tickers()

    print("Total stocks:", len(tickers))

    market_caps = {}

    for i,ticker in enumerate(tickers):
        if i % 50 == 0:
            print("Sleeping for 4 seconds to avoid rate limits...")
            time.sleep(4)
        try:
            ticker_obj = yf.Ticker(ticker)
            market_caps[ticker] = ticker_obj.fast_info.get("marketCap", None)
            print(f"{ticker}: {market_caps[ticker]}")
        except Exception as e:
            print(f"Error fetching {ticker}: {e}")
            market_caps[ticker] = None

    df = pd.DataFrame(list(market_caps.items()), columns=["Stock", "MarketCap"])
    df.to_csv("data/market_caps.csv", index=False)

    print("Market caps saved!")