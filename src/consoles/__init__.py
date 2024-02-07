from pathlib import Path
from consoles.utils.version import get_version

BASE_DIR = Path(__file__).resolve().parent.parent

VERSION = (1, 0, 0, "dev", 0)

__version__ = "1.0-dev"


def setup():
    from consoles.apps import apps
    from consoles.conf import settings
    apps.populate(settings.INSTALLED_APPS)
