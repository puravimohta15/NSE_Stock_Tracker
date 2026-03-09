import pandas as pd

def get_nse_tickers():

    df = pd.read_csv("EQUITY_L.csv")

    tickers = []

    for symbol in df["SYMBOL"]:
        tickers.append(symbol + ".NS")

    return tickers