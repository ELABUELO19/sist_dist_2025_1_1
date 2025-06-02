FROM python:3.9-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV FLASK_APP=api.py
ENV FLASK_ENV=production

RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN adduser --disabled-password --gecos '' appuser

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY api.py .

RUN mkdir -p /app/logs && \
    chown -R appuser:appuser /app

USER appuser

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:5000/api/health || exit 1

CMD ["python", "-m", "gunicorn", "--bind", "0.0.0.0:5000", "--workers", "4", "--timeout", "60", "--access-logfile", "-", "--error-logfile", "-", "api:app"]