FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEBIAN_FRONTEND=noninteractive \
    TZ=America/Sao_Paulo

# Chromium + chromedriver (leve, vem do repo do Debian)
RUN apt-get update && apt-get install -y --no-install-recommends \
      chromium \
      chromium-driver \
      fonts-liberation \
      ca-certificates \
      tzdata \
      tini \
    && rm -rf /var/lib/apt/lists/*

ENV CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER_PATH=/usr/bin/chromedriver \
    HEADLESS=true

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "run_monthly.py"]
