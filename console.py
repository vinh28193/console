import os
import sys
from dotenv import load_dotenv

if __name__ == "__main__":
    load_dotenv()

    os.environ.setdefault("SETTINGS_MODULE", "fasttraders.settings")

    from consoles import setup
    from consoles.management import execute_from_command_line

    setup()
    execute_from_command_line(sys.argv)
