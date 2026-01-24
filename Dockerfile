FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu

USER root

# Install Jupyter and delta spark
RUN pip3 install --no-cache-dir \
        delta-spark==3.1.0 && \
        jupyterlab \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Spark config for Delta (global, no per-session config)
COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

# Working directory for notebooks
WORKDIR /opt/workspace

EXPOSE 8888
EXPOSE 4040

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--no-browser", "--allow-root"]
