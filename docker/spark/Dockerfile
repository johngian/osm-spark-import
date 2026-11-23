# Spark (standalone master, workers, the job driver) with Iceberg, pyosmium,
# and the osm-spark-import package; the same image runs the loader.
FROM python:3.10-slim-bookworm
ARG ICEBERG_VERSION=1.6.1
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless libexpat1 procps curl ca-certificates postgresql-client osmium-tool \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java
COPY requirements.txt constraints.txt /opt/osm-spark-import/
RUN pip install --no-cache-dir -c /opt/osm-spark-import/constraints.txt -r /opt/osm-spark-import/requirements.txt
ENV JAVA_HOME=/opt/java \
    SPARK_HOME=/usr/local/lib/python3.10/site-packages/pyspark \
    SPARK_CONF_DIR=/etc/spark \
    PYSPARK_PYTHON=python3
ENV PATH=$SPARK_HOME/bin:$PATH
RUN curl -fsSL -o "$SPARK_HOME/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"
COPY docker/spark/spark-defaults.conf docker/spark/log4j2.properties /etc/spark/
COPY pyproject.toml LICENSE NOTICE README.md /opt/osm-spark-import/
COPY src /opt/osm-spark-import/src
COPY bin /opt/osm-spark-import/bin
COPY tests /opt/osm-spark-import/tests
COPY mappings /opt/osm-spark-import/mappings
RUN pip install --no-cache-dir -c /opt/osm-spark-import/constraints.txt "/opt/osm-spark-import[test]" \
 && cd /opt/osm-spark-import && python -m pytest -q -p no:cacheprovider tests
WORKDIR /data
