# Python + Chromium for DrissionPage scraping
# Works on both ARM64 (Mac M1/M2) and AMD64 (VPS)
FROM python:3.11-slim

# Install Chromium, curl for health checks, Tini for signal forwarding, and dependencies
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    curl \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    tini \
    xdg-utils \
    --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Chromium path for DrissionPage
ENV CHROME_PATH=/usr/bin/chromium

# Set working directory
WORKDIR /app

# Copy requirements first (for layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Make the startup wrapper executable. It uses exec so Tini can forward stop
# signals directly to Gunicorn, Celery, or the scraper process.
RUN chmod +x /app/docker-entrypoint.sh

# Environment
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV DEPENDENCY_MAX_ATTEMPTS=30
ENV DEPENDENCY_RETRY_MAX_DELAY=10

# Compose overrides this per service. This image-level default covers the web
# process and also documents the container's health contract.
HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
    CMD curl --fail --silent http://127.0.0.1:5000/health || exit 1

STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/bin/tini", "--", "/app/docker-entrypoint.sh"]

# Default command (override in docker-compose)
CMD ["python", "kwscraper.py"]
