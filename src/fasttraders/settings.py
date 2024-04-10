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

# Data
DATA_PATH = '/data/'

# Telegram
TELEGRAM_RELOAD = False
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
