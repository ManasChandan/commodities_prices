FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu

USER root

# Install Jupyter and delta spark
RUN pip3 install --no-cache-dir \
    jupyterlab \
    delta-spark==3.1.0  \
    streamlit

# Ensure directories exist and have correct permissions
RUN mkdir -p /opt/workspace /opt/warehouse /opt/landing && \
    chmod -R 777 /opt/workspace /opt/warehouse /opt/landing

# Spark config for Delta
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Pre-download Delta JARs so they are baked into the image
RUN /opt/spark/bin/spark-shell --packages io.delta:delta-spark_2.12:3.1.0 --eval "sys.exit()"

WORKDIR /opt/workspace

EXPOSE 8888 4040 5050

CMD jupyter lab --ip=0.0.0.0 --no-browser --allow-root --NotebookApp.token='' & \
    streamlit run app.py --server.port 5050 --server.address 0.0.0.0