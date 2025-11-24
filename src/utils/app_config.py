"""Application configuration data structures."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Tuple
import os

from configparser import ConfigParser

from src.utils.helpers import get_script_folder, resource_path

CONFIG_FILENAME = "config.ini"


@dataclass(frozen=True)
class AppDataConfig:
    """Immutable container for application configuration values."""

    environment: str
    username: str
    password: str
    link_up: Tuple[str, ...]
    url: str
    verify_ssl: bool = True
    ca_bundle: str | None = None
    # Optional platform/file-specific path used by some sections (e.g. MPS)
    file_path: Tuple[str, ...] | None = None

    @classmethod
    def from_parser(
        cls,
        parser: ConfigParser,
        section: str | None = None,
    ) -> "AppDataConfig":
        """Create an instance from a ``ConfigParser`` object."""

        # Prefer an explicit section when provided. When no section is
        # specified, prefer the `SPA` section if present (this config
        # format stores the main app settings under `[SPA]`), otherwise
        # fall back to the parser's default section.
        if section:
            section_name = section
            if section_name != parser.default_section and not parser.has_section(
                section_name
            ):
                section_name = parser.default_section
        else:
            section_name = (
                "SPA" if parser.has_section("SPA") else parser.default_section
            )

        get = parser.get
        environment = get(section_name, "environment", fallback="development")
        username = get(section_name, "username", fallback="")
        password = get(section_name, "password", fallback="")
        link_up_raw = get(section_name, "link_up", fallback="")
        url = get(section_name, "url", fallback="")
        verify_ssl = parser.getboolean(section_name, "verify_ssl", fallback=True)
        ca_bundle = get(section_name, "ca_bundle", fallback=None) or None
        file_path_raw = get(section_name, "file_path", fallback=None) or None

        link_up = cls._normalize_links(link_up_raw)
        file_path = cls._normalize_paths(file_path_raw) if file_path_raw else None

        return cls(
            environment=environment.strip(),
            username=username.strip(),
            password=password.strip(),
            link_up=link_up,
            url=url.strip(),
            verify_ssl=verify_ssl,
            ca_bundle=ca_bundle,
            file_path=file_path,
        )

    @staticmethod
    def _normalize_links(value: str) -> Tuple[str, ...]:
        parts: Iterable[str] = (part.strip() for part in value.split(","))
        return tuple(part for part in parts if part)

    @staticmethod
    def _normalize_paths(value: str) -> Tuple[str, ...]:
        """Normalize a comma-separated `file_path` value from the config.

        This will:
        - split on commas,
        - strip surrounding quotes and whitespace,
        - normalize path separators using `os.path.normpath`,
        - return a tuple of non-empty path strings.
        """

        parts: Iterable[str] = (part.strip() for part in value.split(","))
        normalized: list[str] = []
        for part in parts:
            if not part:
                continue
            # Remove surrounding single/double quotes if present
            if (part.startswith('"') and part.endswith('"')) or (
                part.startswith("'") and part.endswith("'")
            ):
                part = part[1:-1]
            # Normalize path separators for the current OS
            part = os.path.normpath(part)
            if part:
                normalized.append(part)

        return tuple(normalized)

    def as_dict(self) -> dict[str, str | Tuple[str, ...] | bool | None]:
        """Expose configuration as a dictionary."""

        return {
            "environment": self.environment,
            "username": self.username,
            "password": self.password,
            "link_up": self.link_up,
            "url": self.url,
            "verify_ssl": self.verify_ssl,
            "ca_bundle": self.ca_bundle,
            "file_path": self.file_path,
        }


def get_config_path() -> Path:
    """Return the absolute path to the application configuration file."""

    return Path(get_script_folder()) / "config" / CONFIG_FILENAME


def create_config(path: Path | None = None) -> Path:
    """Create a default configuration file when none exists."""

    config = ConfigParser()
    link_up = ["LU18", "LU21", "LU26", "LU24"]
    config["DEFAULT"] = {"environment": "production"}

    config["SPA"] = {
        "username": "",
        "password": "",
        "link_up": ",".join(link_up),
        "url": "https://ots.spappa.aws.private-pmideep.biz/db.aspx?",
        # If you are using self-signed certificates or a private CA,
        # set `verify_ssl` to False or provide `ca_bundle` with a path
        # to a PEM file containing your certificate(s).
        "verify_ssl": "False",
        "ca_bundle": "config/ca-bundle.pem",
    }

    config["DH"] = {
        "environment": "development",
    }

    config["MPS"] = {
        "environment": "development",
        # Keep link_up in sync with available default paths
        "link_up": "LU21,LU26",
        # Use comma-separated forward-slash paths (no surrounding quotes)
        "file_path": "D:/Program/Python/tester/assets/21-MPS board Print.xlsx, D:/Program/Python/tester/assets/26-MPS board Print.xlsx",
    }

    target_path = path or get_config_path()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with target_path.open("w", encoding="utf-8") as handle:
        config.write(handle)

    return target_path


def generate_ca_bundle(bundle_path: Path) -> None:
    """Generate the CA bundle file from certificate assets."""

    assets_path = Path(resource_path("assets"))
    sub_ca_path = assets_path / "PMI Sub CA v3.crt"
    aws_ca_path = assets_path / "PMI AWS CA v3.crt"
    spa_ca_path = assets_path / "ots.spappa.aws.private-pmideep.biz.crt"

    if not sub_ca_path.exists() or not aws_ca_path.exists() or not spa_ca_path.exists():
        return  # Skip if certificate files are missing

    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    with bundle_path.open("w", encoding="utf-8") as bundle_file:
        bundle_file.write(sub_ca_path.read_text(encoding="utf-8"))
        bundle_file.write(aws_ca_path.read_text(encoding="utf-8"))
        bundle_file.write(spa_ca_path.read_text(encoding="utf-8"))


def read_config(section: str | None = None) -> AppDataConfig:
    """Load the application configuration data as an ``AppDataConfig``."""

    config_path = get_config_path()
    parser = ConfigParser()

    if not config_path.exists():
        create_config(config_path)

    parser.read(config_path, encoding="utf-8")
    cfg = AppDataConfig.from_parser(parser, section=section)

    # Generate CA bundle if configured and missing
    if cfg.ca_bundle:
        ca_path = Path(cfg.ca_bundle)

        # If the configured path is absolute, use it directly.
        if ca_path.is_absolute():
            bundle_path = ca_path
        else:
            # For relative paths, place the bundle next to the script/exe.
            # `get_script_folder()` already handles PyInstaller frozen apps.
            bundle_path = Path(get_script_folder()) / ca_path

            # If creating directories next to the executable fails (e.g.
            # because the exe lives in a protected location), fall back to
            # a per-user APPDATA location so we can still generate the
            # bundle and have a writable path.
            try:
                bundle_path.parent.mkdir(parents=True, exist_ok=True)
            except Exception:
                appdata_dir = Path(os.getenv("APPDATA") or Path.home())
                bundle_path = appdata_dir / "SPA-Dashboard" / ca_path
                bundle_path.parent.mkdir(parents=True, exist_ok=True)

        if not bundle_path.exists():
            generate_ca_bundle(bundle_path)

    return cfg


def get_base_url(section: str | None = None) -> str:
    """Return the configured base URL (convenience wrapper).

    This central helper ensures other modules obtain the base URL from a
    single place and always receive a non-null string.
    """

    cfg = read_config(section=section)
    return cfg.url or ""
