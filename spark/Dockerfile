FROM apache/spark:3.5.0

USER root

# Create directories and set permissions
RUN mkdir -p /app /opt/spark/data/lake /opt/spark/checkpoint && \
    chmod -R 777 /app /opt/spark/data/lake /opt/spark/checkpoint

# Install Python dependencies
COPY app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

WORKDIR /app

# Switch to default Spark user
USER 185
