# HDFS for local development: one namenode, one or more datanodes.
FROM eclipse-temurin:11-jre-jammy
ARG HADOOP_VERSION=3.4.1
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}-lean.tar.gz" \
    | tar -xz -C /opt \
 && ln -s "/opt/hadoop-${HADOOP_VERSION}" /opt/hadoop \
 && rm -rf /opt/hadoop/share/doc
ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    PATH=/opt/hadoop/bin:/opt/hadoop/sbin:$PATH
COPY core-site.xml hdfs-site.xml /opt/hadoop/etc/hadoop/
COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
