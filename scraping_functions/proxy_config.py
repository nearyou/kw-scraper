"""Environment-backed proxy configuration for scraper workers."""

from dataclasses import dataclass
import os
from typing import Mapping, Optional


@dataclass(frozen=True)
class ProxySettings:
    """Validated credentials for one authenticated upstream proxy."""

    host: str
    port: str
    username: str
    password: str
    scheme: str = "http"
    replace_existing: bool = False


def load_proxy_settings(
    environ: Optional[Mapping[str, str]] = None,
) -> Optional[ProxySettings]:
    """Load a proxy from ``PROXY_*`` variables, or return ``None`` if disabled.

    Credentials are deliberately never included in validation errors or logs.
    """

    values = os.environ if environ is None else environ
    names = ("PROXY_HOST", "PROXY_PORT", "PROXY_USERNAME", "PROXY_PASSWORD")
    configured = {name: str(values.get(name, "")).strip() for name in names}

    if not any(configured.values()):
        return None

    missing = [
        name
        for name in ("PROXY_HOST", "PROXY_PORT", "PROXY_USERNAME", "PROXY_PASSWORD")
        if not configured[name]
    ]
    if missing:
        raise ValueError(f"Incomplete proxy configuration; missing {', '.join(missing)}")

    port = configured["PROXY_PORT"]
    try:
        numeric_port = int(port)
    except ValueError as error:
        raise ValueError("PROXY_PORT must be a number") from error
    if not 1 <= numeric_port <= 65535:
        raise ValueError("PROXY_PORT must be between 1 and 65535")

    scheme = str(values.get("PROXY_SCHEME", "http")).strip().lower() or "http"
    if scheme not in {"http", "https"}:
        raise ValueError("PROXY_SCHEME must be http or https")

    replace_existing = str(values.get("PROXY_REPLACE_EXISTING", "false")).strip().lower()
    if replace_existing not in {"true", "false", "1", "0", "yes", "no"}:
        raise ValueError("PROXY_REPLACE_EXISTING must be true or false")

    return ProxySettings(
        host=configured["PROXY_HOST"],
        port=str(numeric_port),
        username=configured["PROXY_USERNAME"],
        password=configured["PROXY_PASSWORD"],
        scheme=scheme,
        replace_existing=replace_existing in {"true", "1", "yes"},
    )
