import requests
import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_TOKEN").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID").strip()

def send_alert(message):

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": CHAT_ID,
        "text": message
    }
    r = requests.post(url, data=payload)
    print("Telegram response:", r.json())


