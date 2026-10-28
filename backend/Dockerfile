FROM python:3.12-slim

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt greenlet

COPY . .

EXPOSE 8000

# --no-access-log: a per-request log line is measurable CPU on a CPU-bound tier (NGINX already logs)
# keep-alive must outlive NGINX's upstream keepalive (60s) or reused connections get reset mid-request
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--timeout-keep-alive", "75", "--no-access-log"]
