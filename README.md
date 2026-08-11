# KW Scraper

Scraper for Polish land & mortgage registers (Przeglądarka Ksiąg Wieczystych, `ekw.ms.gov.pl`) plus a Flask web panel to browse the scraped data.

Scraped records are stored in **PostgreSQL** (structured data parsed from the raw HTML). No upstream object storage is used.

## Architecture

- **`scraping_functions/scraper.py`** — DrissionPage/Chromium scraper with residential-proxy rotation, anti-bot (Incapsula/WAF) handling and human-like delays.
- **`kwscraper.py`** — parallel downloader/parser pipeline (`download_worker`), bounded in-flight window.
- **`kwparser.py` / `ekwparser.py`** — parse the saved HTML sections into the DB models.
- **`www/`** — Flask app (web admin panel, Celery tasks, queue manager, idle scraping).
- **`migrations/`** — Flask-Migrate/Alembic migrations.

## Requirements

- Python 3.11+ (developed and tested on 3.13)
- PostgreSQL
- RabbitMQ (Celery broker)
- Chrome/Chromium (for DrissionPage)

## Environment variables

Copy `.env.example` to `.env` and fill in the values. Required:

| Variable | Description |
|---|---|
| `DATABASE_URL` | SQLAlchemy URL, e.g. `postgresql+psycopg2://user:pass@localhost/kwscraper` |
| `APP_SECRET` | Flask session secret (random string) |
| `CELERY_BROKER_URL` | e.g. `amqp://guest:guest@localhost:5672//` |

`TEST_MODE=1|true|yes|y` runs scraping **without proxies** — for development only (your IP may get blocked).

## Setup

```bash
# 1) create venv and install deps
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt

# 2) configure
cp .env.example .env            # then edit .env

# 3) create PostgreSQL database, then tables
python init_db.py
# or, if you prefer migrations: flask db upgrade

# 4) create admin user
flask --app www.main shell
```

```python
from www.models import db, User
admin = User(login='admin', is_admin=True)
admin.set_password('replace_with_a_strong_password')
db.session.add(admin)
db.session.commit()
```

## Running

Start the queue manager / scrapers (RabbitMQ and PostgreSQL must be up):

```bash
# Celery worker (runs `queue_manager_task` which processes TaskQueue entries)
celery -A www.main.celery worker --loglevel=error --concurrency=4

# Web panel (dev)
flask --app www.main run --port 8000
# or production-style
gunicorn www.main:app --bind 0.0.0.0:8000 --worker-class gthread --workers 2 --threads 2
```

The terminal shows errors only by default. Detailed INFO events remain available
as rotating JSON files under `logs/`; set `CONSOLE_LOG_LEVEL=INFO` temporarily
when interactive progress output is needed.

The queue manager starts automatically (see `@celery.on_after_configure` in `www/main.py`); it also self-restarts if it stops. Regular scraping tasks are created through the web panel (`TaskQueue`). When the queue is empty, **idle scraping** (`scrape_idle_task`) processes court codes from `www/codes.json` alphabetically.

Scraping in Docker is supported via `docker-compose.yml` (`celery`, `web`, `rabbitmq`; the `scraper` profile runs an ad-hoc scraper). Variabel `DB_PASSWORD` is read from your environment for the compose DB URL.

### Anti-Detection / Rate limiting

- `RATE_LIMIT_ENABLED`, `RATE_LIMIT_MIN`, `RATE_LIMIT_MAX`
- `BOOK_DELAY_MIN`, `BOOK_DELAY_MAX`, `SECTION_DELAY_MIN`, `SECTION_DELAY_MAX`, `SEARCH_DELAY_MIN`, `SEARCH_DELAY_MAX`
- `DOWNLOAD_WORKERS`, `PARSING_WORKERS`, `IN_FLIGHT_LIMIT` — concurrency knobs.

## Idle alphabetical scraping

When the task queue is empty, the app can automatically scrape books alphabetically by court code from `www/codes.json`.

- `IDLE_SCRAPE_ENABLED` (`true`) — master switch for idle scraping.
- `IDLE_EMPTY_STREAK_LIMIT` (default `40000`) — how many consecutive "not-found" results before moving to the next court.
- `IDLE_DOWNLOAD_WORKERS` (defaults to `DOWNLOAD_WORKERS`) — idle worker count.
- Idle ranges are stored in `IdleTasks` and resume from `last_processed`.

## Maintenance Mode Detection

The scraper automatically detects when the target website is in maintenance mode and halts all scraping activities until the site becomes available again.

- `MAINTENANCE_CHECK_INTERVAL` (default `21600`, 6 h) — how often to re-check while in maintenance mode.
- Detected by: `przerwa serwisowa` text, missing search-form elements, server errors, missing page elements.
- When the site comes back, scraping resumes automatically.

## Project layout

```
kwscraper.py            parallel download+parse pipeline (entry point: python kwscraper.py)
kwparser.py             KW parsing + DB writes
ekwparser.py            legacy EKW parser
department_codes.py     court codes fallback
helper.py               book number/control-digit helpers
scraping_functions/     DrissionPage scraper
www/                    Flask app + Celery + queue manager + templates
migrations/             Alembic migrations
docker-compose.yml      celery + web + rabbitmq (+ optional scraper profile)
```

> **Note for contributors:** proxying is handled via Chromium proxy-auth extensions; proxies are stored in the `proxies` table and managed in the web panel (see `www/main.py`). Running with `TEST_MODE=1` bypasses the proxy requirement for local development.
