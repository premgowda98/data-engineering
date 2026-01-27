FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu

USER root

# Install Python + Jupyter
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
      numpy \
      pandas \
      ipython && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create writable dirs for spark user
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
