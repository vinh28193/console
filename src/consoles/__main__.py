import sys


def main() -> None:
    from consoles import management, setup
    setup()
    sys.exit(management.execute_from_command_line())


if __name__ == "__main__":
    main()
