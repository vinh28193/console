# List of strings representing installed apps.
INSTALLED_APPS = [
    "fasttraders"
]

STRATEGIES = {
    'default': {
        'ENGINE': 'fasttraders.strategies.backends.simple',
    }
}


