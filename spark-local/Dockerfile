FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu

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
      pyspark==3.5.1 \
      delta-spark==3.1.0 \
      numpy \
      pandas \
      ipython && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# -----------------------------
# ADD DELTA LAKE JARS
# -----------------------------
RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/delta-spark_2.12-3.1.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar && \
    curl -L -o /opt/spark/jars/delta-storage-3.1.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar

# -----------------------------
# Spark defaults (Delta config)
# -----------------------------
RUN mkdir -p /opt/spark/conf && \
    echo "spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.sql.warehouse.dir /data/warehouse" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.databricks.delta.retentionDurationCheck.enabled false" >> /opt/spark/conf/spark-defaults.conf

# -----------------------------
# Writable dirs
# -----------------------------
RUN mkdir -p \
      /home/spark/.jupyter \
      /home/spark/.local/share/jupyter \
      /opt/spark-notebooks && \
    chown -R spark:spark /home/spark && \
    chown -R spark:spark /opt/spark-notebooks

USER spark
ENV SHELL=/bin/bash
ENV HOME=/home/spark

WORKDIR /opt/spark-notebooks

EXPOSE 8888 4040

CMD ["python3", "-m", "jupyterlab", \
     "--ip=0.0.0.0", \
     "--port=8888", \
     "--no-browser", \
     "--ServerApp.token=''"]
