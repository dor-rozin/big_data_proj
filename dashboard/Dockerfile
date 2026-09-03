FROM python:3.11-slim

# docker-cli + docker-compose: the "Refresh data" button shells out to
# `docker compose --profile jobs run --rm spark` on the HOST's daemon (the
# socket is bind-mounted in docker-compose.yml, Docker-out-of-Docker style) --
# this is not a container-in-container, it launches a sibling container next
# to this one.
RUN apt-get update && apt-get install -y --no-install-recommends \
    docker-cli docker-compose \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

EXPOSE 8501
# Bind to 0.0.0.0 so the browser on the host can reach the container.
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]
