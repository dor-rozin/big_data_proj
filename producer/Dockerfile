# Build context is the REPO ROOT, not ./producer -- the producer validates every
# message against schemas/*.schema.json before sending, so the frozen contract
# has to be inside the image. See the producer service's `build:` block in
# docker-compose.yml.
FROM python:3.11-slim

WORKDIR /app

COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer/ /app/
COPY schemas/ /app/schemas/
COPY scripts/create_topics.py scripts/describe_topics.py /app/scripts/

# Unbuffered so the progress line and summary show up in `docker compose logs`
# as they happen, rather than all at once when the process exits.
ENV PYTHONUNBUFFERED=1

# Snapshot parquet files are mounted here read-only by compose.
ENV SNAPSHOT_DIR=/snapshots

CMD ["python", "produce.py"]
