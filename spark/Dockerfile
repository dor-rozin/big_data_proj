FROM python:3.11-slim

# Spark needs a JVM. procps provides `ps`, which Spark calls on startup.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

# The Kafka connector jar is resolved at runtime via spark.jars.packages
# (see pipeline.py) and cached in /root/.ivy2 (mounted volume) between runs.
CMD ["python", "pipeline.py"]
