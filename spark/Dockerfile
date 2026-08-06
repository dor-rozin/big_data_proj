FROM python:3.11-slim

# Spark needs a JVM. procps provides `ps`, which Spark calls on startup.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

# Python block-buffers stdout when it is a pipe rather than a terminal, so
# `docker compose run ... | grep` holds every progress line in an 8 KB buffer
# until the process exits. A job that is waiting on something then looks
# identical to a job that has hung: the JVM's log4j output keeps arriving on
# stderr while the pipeline's own stage banners are invisible. Unbuffering costs
# nothing here and is the difference between diagnosing a stall in seconds and
# guessing at it.
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

# The Kafka connector jar is resolved at runtime via spark.jars.packages
# (see pipeline.py) and cached in /root/.ivy2 (mounted volume) between runs.
CMD ["python", "pipeline.py"]
