# List of strings representing installed apps.
INSTALLED_APPS = [
    "fasttraders"
]

STRATEGIES = {
    'default': {
        'ENGINE': 'fasttraders.strategies.backends.simple',
    }
}

TELEGRAM_BOT_TOKEN = '6081142508:AAG-JaKUJMqHWDSoDiDIyp4CyqoIe4JHkZM'
TELEGRAM_CHAT_ID = '-670681421'
