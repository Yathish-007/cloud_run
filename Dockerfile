FROM openjdk:17-jdk-slim

WORKDIR /app

# Install Python + tools
RUN apt-get update && apt-get install -y python3 python3-pip curl && rm -rf /var/lib/apt/lists/*

# Install Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar zx -C /opt && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark
ENV PATH="/opt/spark/bin:${PATH}"

# BigQuery connector
ADD https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.38.0.jar /opt/spark/jars/

# Copy project files
COPY . /app

# Install Python deps (Flask, requests, pyspark, python-dotenv, google-cloud-bigquery if needed)
RUN pip3 install --no-cache-dir -r requirements.txt

ENV PORT=8080
EXPOSE 8080

CMD ["python3", "app.py"]
