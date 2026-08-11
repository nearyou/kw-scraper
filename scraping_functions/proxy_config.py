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


def load_proxy_settings(
    environ: Optional[Mapping[str, str]] = None,
) -> Optional[ProxySettings]:
    """Load a proxy from ``PROXY_*`` variables, or return ``None`` if disabled.

    Bright Data's shared superproxy port is 33335, so it is used when a host is
    configured without an explicit port. Credentials are deliberately never
    included in validation errors or logs.
    """

    values = os.environ if environ is None else environ
    names = ("PROXY_HOST", "PROXY_PORT", "PROXY_USERNAME", "PROXY_PASSWORD")
    configured = {name: str(values.get(name, "")).strip() for name in names}

    if not any(configured.values()):
        return None

    missing = [
        name
        for name in ("PROXY_HOST", "PROXY_USERNAME", "PROXY_PASSWORD")
        if not configured[name]
    ]
    if missing:
        raise ValueError(f"Incomplete proxy configuration; missing {', '.join(missing)}")

    port = configured["PROXY_PORT"] or "33335"
    try:
        numeric_port = int(port)
    except ValueError as error:
        raise ValueError("PROXY_PORT must be a number") from error
    if not 1 <= numeric_port <= 65535:
        raise ValueError("PROXY_PORT must be between 1 and 65535")

    scheme = str(values.get("PROXY_SCHEME", "http")).strip().lower() or "http"
    if scheme not in {"http", "https"}:
        raise ValueError("PROXY_SCHEME must be http or https")

    return ProxySettings(
        host=configured["PROXY_HOST"],
        port=str(numeric_port),
        username=configured["PROXY_USERNAME"],
        password=configured["PROXY_PASSWORD"],
        scheme=scheme,
    )
