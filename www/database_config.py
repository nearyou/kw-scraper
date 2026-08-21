"""Database URL and pool settings shared by startup and tests."""


def build_database_uri(database_url):
    if not database_url:
        raise RuntimeError("DATABASE_URL is required")
    if not database_url.startswith(("postgresql://", "postgresql+psycopg2://")):
        return database_url
    if "sslmode=" in database_url:
        return database_url
    separator = "&" if "?" in database_url else "?"
    return f"{database_url}{separator}sslmode=disable"


def database_engine_options(database_url):
    if database_url.startswith("sqlite:"):
        return {"pool_pre_ping": True}
    return {
        "pool_size": 10,
        "max_overflow": 5,
        "pool_timeout": 30,
        "pool_recycle": 300,
        "pool_pre_ping": True,
    }
