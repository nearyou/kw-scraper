# KW Scraper

## Environment Variables

The following environment variables can be set in a `.env` file or in your system environment:

- `MONGO_URI`: MongoDB connection string (default: `mongodb://localhost:27017/proxy_db`)
- `OXYLABS_USER`: Oxylabs username for proxy
- `OXYLABS_PASS`: Oxylabs password for proxy
- `TEST_MODE`: Set to `1`, `true`, `yes`, or `y` to run without proxies (for development and testing only)

## Running in Test Mode

For development and testing purposes, you can run the scraper without using proxies by setting the `TEST_MODE` environment variable:

```bash
# In Linux/macOS:
export TEST_MODE=1
python kwscraper.py

# In Windows Command Prompt:
set TEST_MODE=1
python kwscraper.py

# In Windows PowerShell:
$env:TEST_MODE=1
python kwscraper.py
```

⚠️ **WARNING**: Running without proxies may lead to your IP being blocked by the target website. This mode is intended only for development and testing.

## Normal Operation

For normal operation, ensure you have proper proxy configuration set up in MongoDB or provide OxyLabs credentials via environment variables.

```bash
python kwscraper.py
```

## Getting started
```
celery -A main.celery worker --loglevel=info
sudo systemctl reload postgresql
sudo systemctl restart rabbitmq-server

flask --app main.py shell

from models import db, User
username = 'admin' 
password = 'qwe12345'
new_user = User(login=username)
new_user.set_password(password)
db.session.add(new_user)
db.session.commit()
```

## Idle alphabetical scraping

When the task queue is empty, the app can automatically scrape books alphabetically by court codes from `www/codes.json`.

Behavior:

Environment variables:
 After `IDLE_EMPTY_STREAK_LIMIT` consecutive "not-found" results for a court (default 40000), it moves to the next court.
 `IDLE_EMPTY_STREAK_LIMIT` (default `40000`) — how many consecutive not-found before switching court.

Note: idle scraping uses the same proxies/Splash configuration as normal tasks.

## Maintenance Mode Detection

The scraper automatically detects when the target website is in maintenance mode and halts all scraping activities until the site becomes available again.

**How it works:**
- Splash script detects maintenance indicators like "przerwa serwisowa" text or missing form elements
- When maintenance is detected, all scraping (regular tasks and idle) is halted
- System performs health checks every 6 hours to test if the site is back online
- Scraping automatically resumes when the site becomes available

**Environment variables:**
- `MAINTENANCE_CHECK_INTERVAL` (default `21600`) — seconds between health checks when in maintenance mode (6 hours)

**Maintenance indicators detected:**
- "przerwa serwisowa" text on pages
- Missing search form inputs
- Server error responses
- Essential page elements not loading