FROM apache/spark:4.0.1-scala2.13-java17-python3-r-ubuntu

USER root

# -----------------------------
# Install Python + Jupyter
# -----------------------------
RUN apt-get update && \
    apt-get install -y \
      python3 \
      python3-pip \
      python3-dev \
      build-essential \
      vim nano bash-completion && \
    pip3 install --no-cache-dir \
      jupyterlab \
      pyspark==4.0.1 \
      delta-spark>=4.0.0 \
      numpy \
      pandas \
      confluent_kafka \
      avro \
      fastavro \
      ipython && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# -----------------------------
# Spark defaults (Delta + Kafka + Avro)
# -----------------------------
RUN mkdir -p /opt/spark/conf && \
    echo "spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.databricks.delta.retentionDurationCheck.enabled false" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.jars.packages io.delta:delta-spark_2.13:4.0.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.apache.spark:spark-avro_2.13:4.0.1" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.jars.ivy /home/spark/.ivy2" >> /opt/spark/conf/spark-defaults.conf

# -----------------------------
# Writable dirs (CRITICAL for Spark 4)
# -----------------------------
RUN mkdir -p \
      /home/spark/.ivy2 \
      /home/spark/.jupyter \
      /home/spark/.local/share/jupyter \
      /opt/spark-notebooks && \
    chown -R spark:spark /home/spark && \
    chown -R spark:spark /opt/spark-notebooks

# -----------------------------
# Runtime user
# -----------------------------
USER spark
ENV SHELL=/bin/bash
ENV HOME=/home/spark
ENV IVY_HOME=/home/spark/.ivy2

WORKDIR /opt/spark-notebooks

EXPOSE 8888 4040

CMD ["python3", "-m", "jupyterlab", \
     "--ip=0.0.0.0", \
     "--port=8888", \
     "--no-browser", \
     "--ServerApp.token=''"]
