FROM openjdk:8-jdk


RUN apt-get update && apt-get install -y \
    wget \
    curl \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*


ENV HADOOP_VERSION=3.2.1
ENV PIG_VERSION=0.17.0
ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV PIG_HOME=/opt/pig-${PIG_VERSION}
ENV JAVA_HOME=/usr/local/openjdk-8
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PIG_HOME/bin


RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} /opt/ && \
    rm hadoop-${HADOOP_VERSION}.tar.gz


RUN wget https://archive.apache.org/dist/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz && \
    tar -xzf pig-${PIG_VERSION}.tar.gz && \
    mv pig-${PIG_VERSION} /opt/ && \
    rm pig-${PIG_VERSION}.tar.gz


RUN mkdir -p /pig_scripts /data /output


COPY hadoop-conf/* $HADOOP_HOME/etc/hadoop/


WORKDIR /pig_scripts


CMD ["tail", "-f", "/dev/null"]