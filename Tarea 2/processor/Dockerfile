FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    wget \
    curl \
    ca-certificates \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN python -m pip install --no-cache-dir --upgrade pip && \
    python -m pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY processor.py .

# Create necessary directories
RUN mkdir -p /app/data /app/output

# Set environment variables for Hadoop (remote connection)
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Run the processor
CMD ["python", "processor.py"]