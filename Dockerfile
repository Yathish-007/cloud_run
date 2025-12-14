FROM eclipse-temurin:17-jdk-jammy

WORKDIR /app

# Install Python and tools
RUN apt-get update && apt-get install -y python3 python3-pip curl && rm -rf /var/lib/apt/lists/*

# Tell Spark which Python to use (driver + executors)
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Install Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar zx -C /opt && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark
ENV PATH="/opt/spark/bin:${PATH}"

# BigQuery connector JAR
ADD https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.38.0.jar /opt/spark/jars/

# Copy project
COPY . /app

# Python deps
RUN pip3 install --no-cache-dir -r requirements.txt

ENV PORT=8080
EXPOSE 8080

CMD ["python3", "app.py"]
