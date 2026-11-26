"""Enable ``python -m webhook_delivery <command>`` for the operations CLI."""

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
