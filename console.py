import os
import sys

if __name__ == "__main__":
    os.environ.setdefault("SETTINGS_MODULE", "settings")
    from consoles import setup
    from consoles.management import execute_from_command_line
    setup()
    execute_from_command_line(sys.argv)
