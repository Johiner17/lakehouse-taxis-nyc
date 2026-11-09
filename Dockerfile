FROM python:3.12-slim-bookworm

# Spark corre sobre la JVM, así que necesita Java.
# procps lo usa Spark internamente (comando "ps").
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt