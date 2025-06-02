FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    openjdk-11-jdk \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

ENV HADOOP_VERSION=3.2.1
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} /opt/hadoop && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY processor.py .

RUN mkdir -p /app/data /app/output

CMD ["python", "processor.py"]