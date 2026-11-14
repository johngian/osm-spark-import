# Spark (standalone master, workers, the job driver) with Iceberg, pyosmium,
# and the osm-hadoop package; the same image runs the loader.
FROM python:3.11-slim-bookworm
ARG SPARK_VERSION=3.5.3
ARG ICEBERG_VERSION=1.6.1
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless libexpat1 procps curl ca-certificates postgresql-client osmium-tool \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java
RUN pip install --no-cache-dir "pyspark==${SPARK_VERSION}"
ENV JAVA_HOME=/opt/java \
    SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark \
    SPARK_CONF_DIR=/etc/spark \
    PYSPARK_PYTHON=python3
ENV PATH=$SPARK_HOME/bin:$PATH
RUN curl -fsSL -o "$SPARK_HOME/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"
COPY docker/spark/spark-defaults.conf docker/spark/log4j2.properties /etc/spark/
COPY jobs /opt/osm-hadoop/jobs
RUN pip install --no-cache-dir "/opt/osm-hadoop/jobs[test]" \
 && cd /opt/osm-hadoop/jobs && python -m pytest -q tests
WORKDIR /data
