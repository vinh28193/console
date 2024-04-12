import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# List of strings representing installed apps.
INSTALLED_APPS = [
    "fasttraders"
]

# Worker
PROCESS_THROTTLE_SECS = 5
HEARTBEAT_INTERVAL = 60

# BOT
# Bot initialization with state name
BOT_INIT_STATE = 'STARTED'

# Pair
PAIR_WILDCARDS = [
    "BTC/USDT",
    "LTC/USDT",
    "ETH/USDT"
]
# Data
DATA_PATH = os.path.join(os.path.dirname(os.path.dirname(BASE_DIR)), 'data')

# Telegram
TELEGRAM_RELOAD = False
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
