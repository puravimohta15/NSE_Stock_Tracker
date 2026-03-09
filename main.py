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

        bullish = signals[signals["Signal"] == "Bullish"]
        bearish = signals[signals["Signal"] == "Bearish"]

        # Bullish message
        if not bullish.empty:
            bullish_msg = "📈 Bullish EMA Crossovers\n\n"
            for _, row in bullish.iterrows():
                bullish_msg += f"{row['Stock']} | RSI {row['RSI']}\n"

            print("Sending bullish alerts:\n", bullish_msg)
            send_alert(bullish_msg)

        # Bearish message
        if not bearish.empty:
            bearish_msg = "📉 Bearish EMA Crossovers\n\n"
            for _, row in bearish.iterrows():
                bearish_msg += f"{row['Stock']} | RSI {row['RSI']}\n"

            print("Sending bearish alerts:\n", bearish_msg)
            send_alert(bearish_msg)
