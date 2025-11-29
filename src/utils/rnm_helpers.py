import re


def sanitize_linkup(value: str) -> str:
    """Remove a leading 'LU' (case-insensitive) from a linkup string.

    This is safer than naively using str.strip which removes characters anywhere.
    """
    if not value:
        return ""

    return re.sub(r"^LU", "", str(value), flags=re.IGNORECASE).strip()
