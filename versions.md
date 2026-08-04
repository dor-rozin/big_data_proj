# Pinned versions

Resolved image tags and language/package versions this project is built and
tested against. Paste this file in as context for any AI coding assistant
prompt in this repo — the pins matter more than "whatever is current."

## Docker images (`docker-compose.yml`)

| Service      | Image                                              | Tag pinned? |
|--------------|-----------------------------------------------------|-------------|
| kafka        | `apache/kafka`                                       | `3.9.0`     |
| elasticsearch| `docker.elastic.co/elasticsearch/elasticsearch`      | `8.15.3`    |
| kafka-ui     | `provectuslabs/kafka-ui`                             | `latest` (deliberate — no versioned tags published) |

No Kibana service — not needed for this project; kafka-ui + a Streamlit
dashboard cover the "is the data there / does it look right" needs.

## Security tradeoff

`xpack.security.enabled=false` on Elasticsearch, no TLS, no auth anywhere in
the stack. This is a deliberate tradeoff for a local, free, course project —
not an oversight. Do not carry this configuration into anything
internet-facing.

## Python

- **3.11**, matching the `python:3.11-slim` base image in `producer/Dockerfile`
  and `spark/Dockerfile`. `requirements-dev.txt` pins the producer-half local
  dev venv to the same version.
- `spark/requirements.txt` pins `numpy==1.26.4` / `pandas==2.2.2`, neither of
  which ships 3.14 wheels yet — do not bump the venv's Python past 3.11 without
  checking this again.
- `producer/requirements.txt` no longer contains `yfinance` or `kafka-python` at
  all: the producer replays snapshot files rather than fetching, and uses
  `confluent-kafka==2.5.3`. `yfinance==1.5.2` lives only in
  `requirements-dev.txt`, where the snapshot-fetch scripts use it. (The
  `0.2.40` that used to be pinned in the container could not fetch anything —
  Yahoo changed its endpoints after that release.)
- Container vs dev-venv pandas differ on purpose: `producer/requirements.txt`
  pins `pandas==2.2.2` / `pyarrow==16.1.0` to stay aligned with `spark/`, while
  the dev venv runs `pandas==3.0.5` / `pyarrow==25.0.0`. `produce.py` is
  verified on both.
