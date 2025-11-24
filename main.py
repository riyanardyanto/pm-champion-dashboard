import sys
from src.app import App
from src.utils.app_config import read_config
from async_tkinter_loop import async_mainloop  # pyright: ignore[reportMissingTypeStubs]


def main():
    try:
        # Ensure config file exists on first run (will create default if missing)
        read_config()

        app = App()
        async_mainloop(app.root)
    except Exception as exc:  # pragma: no cover - GUI runtime
        print("Error running app:", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
