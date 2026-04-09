FROM apache/spark:3.5.5

USER root

RUN apt-get update && apt-get install -y --no-install-recommends wget \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_JARS_DIR=/opt/spark/jars
ENV MAVEN=https://repo1.maven.org/maven2

# Kafka connector + PostgreSQL JDBC driver
RUN wget -q -P ${SPARK_JARS_DIR} \
    "${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar" && \
    wget -q -P ${SPARK_JARS_DIR} \
    "${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar" && \
    wget -q -P ${SPARK_JARS_DIR} \
    "${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" && \
    wget -q -P ${SPARK_JARS_DIR} \
    "${MAVEN}/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar" && \
    wget -q -P ${SPARK_JARS_DIR} \
    "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"

COPY streaming_job.py /opt/spark-app/streaming_job.py

USER spark

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "spark://spark-master:7077", \
     "--deploy-mode", "client", \
     "--name", "iot-streaming-job", \
     "--conf", "spark.sql.streaming.checkpointLocation=/tmp/spark-checkpoint", \
     "--conf", "spark.streaming.stopGracefullyOnShutdown=true", \
     "/opt/spark-app/streaming_job.py"]
