import sys
from pathlib import Path


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
