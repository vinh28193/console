import sys


def main() -> None:
    from consoles.management import execute_from_command_line
    sys.exit(execute_from_command_line())


if __name__ == "__main__":
    main()
