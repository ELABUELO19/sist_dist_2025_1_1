FROM python:3.9-slim


RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*


WORKDIR /app


COPY requirements.txt .


RUN pip install --no-cache-dir -r requirements.txt


COPY scraper.py .


RUN mkdir -p /app/data


CMD ["python", "scraper.py"]