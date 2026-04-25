FROM apache/spark:3.5.0

USER root

# Create directories
RUN mkdir -p /opt/spark-jobs /opt/spark/jars/extra

# Copy JARs from spark-jars folder (must be present on host before building)
COPY spark-jars/spark-sql-kafka-0-10_2.12-3.5.0.jar /opt/spark/jars/extra/
COPY spark-jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar /opt/spark/jars/extra/
COPY spark-jars/kafka-clients-3.4.1.jar /opt/spark/jars/extra/
COPY spark-jars/commons-pool2-2.11.1.jar /opt/spark/jars/extra/
COPY spark-jars/postgresql-42.7.3.jar /opt/spark/jars/extra/

# Copy the streaming job
COPY spark-jobs/streaming_consumer.py /opt/spark-jobs/streaming_consumer.py

RUN chmod -R 755 /opt/spark/jars/extra/ && \
    chmod 755 /opt/spark-jobs/streaming_consumer.py

USER spark
