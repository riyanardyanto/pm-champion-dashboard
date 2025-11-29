import sys
from pathlib import Path
import warnings
import polars as pl


def resource_path(relative_path: str) -> str:
    """
    Get the absolute path to a resource, compatible with PyInstaller.

    Args:
        relative_path (str): The relative path to the resource.

    Returns:
        str: The absolute path to the resource.
    """
    base_path = getattr(sys, "_MEIPASS", str(Path(".").resolve()))
    return str(Path(base_path) / relative_path)


def get_script_folder() -> str:
    """
    Get the absolute path to the script folder, compatible with PyInstaller.

    Returns:
        str: The absolute path to the script folder.
    """
    if getattr(sys, "frozen", False):
        # When frozen by PyInstaller, prefer the folder containing the
        # original executable (sys.argv[0]) if available. In some
        # PyInstaller modes `sys.executable` points to a temporary
        # extracted binary; writing logs there means they disappear when
        # the temp dir is cleaned. Using argv[0] keeps logs next to the
        # original exe which is what users expect.
        try:
            exe_path = Path(sys.argv[0]).resolve()
            if exe_path.exists():
                return str(exe_path.parent)
        except Exception:
            # Fall back to sys.executable parent if anything goes wrong
            return str(Path(sys.executable).parent)

    return str(Path(sys.modules["__main__"].__file__).resolve().parent)


def safe_read_excel(*args, **kwargs) -> pl.DataFrame:
    """Read Excel via polars while suppressing dtype inference warnings.

    Polars sometimes emits "Could not determine dtype for column N" when it
    encounters mixed/empty columns during schema inference. This helper will
    perform the read while filtering those specific warnings so they don't
    clutter logs. Callers should still validate/clean the resulting frame.
    """
    import os
    import sys

    # Some underlying readers print dtype-detection messages to stderr.
    # Temporarily redirect stderr to devnull while we read so the user isn't
    # spammed with those messages. We still let real errors propagate.
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", message=r"Could not determine dtype for column"
        )
        devnull = open(os.devnull, "w")
        old_stderr = sys.stderr
        try:
            sys.stderr = devnull
            return pl.read_excel(*args, **kwargs)
        finally:
            sys.stderr = old_stderr
            devnull.close()


def safe_read_csv(*args, **kwargs) -> pl.DataFrame:
    """Read CSV via polars while suppressing dtype inference warnings.

    Mirrors safe_read_excel behavior but for CSV ingestion.
    """
    import os
    import sys

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", message=r"Could not determine dtype for column"
        )
        devnull = open(os.devnull, "w")
        old_stderr = sys.stderr
        try:
            sys.stderr = devnull
            return pl.read_csv(*args, **kwargs)
        finally:
            sys.stderr = old_stderr
            devnull.close()
