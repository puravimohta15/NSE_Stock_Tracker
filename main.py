from get_tickers import get_nse_tickers
from scripts.scanner import scan_stocks
from alerts.telegram_alerts import send_alert
import pandas as pd

def chunk_dataframe(df, size):
    for i in range(0, len(df), size):
        yield df.iloc[i:i + size]


if __name__ == "__main__":
    print("Loading tickers...")

    tickers = get_nse_tickers()

    print("Total stocks:", len(tickers))

    signals = scan_stocks(tickers)

    signals.to_csv("data/signals.csv", index=False)

    print("Signals saved!")
    print(signals)
    if not signals.empty:

        buy = signals[signals["Signal"] == "BUY"]
        exit = signals[signals["Signal"] == "EXIT"]

        # BUY ALERTS
        if not buy.empty:
            for chunk in chunk_dataframe(buy, 90):

                buy_msg = "📈 BUY TRIGGER\n\n"

                for stock, rsi in zip(chunk["Stock"], chunk["RSI"]):
                    buy_msg += f"{stock} | RSI {rsi}\n"

                print("Sending buy alerts:\n", buy_msg)
                send_alert(buy_msg)

        # EXIT ALERTS
        if not exit.empty:
            for chunk in chunk_dataframe(exit, 90):

                exit_msg = "📉 EXIT TRIGGER\n\n"

                for stock, rsi in zip(chunk["Stock"], chunk["RSI"]):
                    exit_msg += f"{stock} | RSI {rsi}\n"

                print("Sending exit alerts:\n", exit_msg)
                send_alert(exit_msg)
