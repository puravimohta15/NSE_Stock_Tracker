from get_tickers import get_nse_tickers
from scripts.scanner import scan_stocks
from alerts.telegram_alerts import send_alert

if __name__ == "__main__":
    print("Loading tickers...")

    tickers = get_nse_tickers()

    print("Total stocks:", len(tickers))

    signals = scan_stocks(tickers)

    signals.to_csv("data/signals.csv", index=False)

    print("Signals saved!")
    print(signals)
    if not signals.empty:

        message = "📊 EMA Crossover Signals\n\n"

        for _, row in signals.iterrows():
            message += f"{row['Stock']} {row['Signal']} crossover | RSI {row['RSI']}\n"

        print("Sending alerts:\n", message)
        send_alert(message)
