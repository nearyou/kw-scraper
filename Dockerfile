# Python + Chromium for DrissionPage scraping
# Works on both ARM64 (Mac M1/M2) and AMD64 (VPS)
FROM python:3.11-slim

# Install Chromium (cross-platform), curl for health checks, and dependencies
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

# Create directory for proxy auth extensions
RUN mkdir -p /tmp

# Environment
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Default command (override in docker-compose)
CMD ["python", "kwscraper.py"]
