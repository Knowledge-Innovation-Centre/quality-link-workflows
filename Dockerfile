FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y --auto-remove build-essential

COPY flows/ ./flows/
COPY tasks/ ./tasks/
COPY source_types/ ./source_types/
COPY schema/ ./schema/
COPY prefect.yaml ./
COPY docker/entrypoint-worker.sh /app/docker/entrypoint-worker.sh
RUN chmod +x /app/docker/entrypoint-worker.sh

EXPOSE 4200

CMD ["prefect", "server", "start", "--host", "0.0.0.0"]
