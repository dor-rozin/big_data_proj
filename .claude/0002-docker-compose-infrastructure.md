---
id: 0002
title: Docker Compose stack — Kafka (KRaft), Elasticsearch, kafka-ui
status: done
layer: infrastructure
priority: P0
depends_on: []
---

## Goal
Every downstream ticket, in both halves of the project, assumes `docker compose up`
produces a working Kafka broker reachable from both the host and other containers,
plus an Elasticsearch node. Misconfigured Kafka advertised
listeners are the single largest time sink in projects of this shape: the broker
starts, the logs look clean, and the consumer half's Spark job fails to connect in
a way that looks like *their* bug. Getting this exactly right, once, on day one, is
worth more than any other single piece of work in this repo.

## Scope
- **`docker-compose.yml`** with infrastructure services on one user-defined
  bridge network (plus `producer`/`spark`/`dashboard`, already present):
  - `kafka` — image `apache/kafka:3.9.0`, KRaft mode (no Zookeeper), single node
    acting as both broker and controller.
  - `elasticsearch` — image `docker.elastic.co/elasticsearch/elasticsearch:8.15.3`,
    `discovery.type=single-node`, `xpack.security.enabled=false`,
    `ES_JAVA_OPTS=-Xms1g -Xmx1g`.
  - `kafka-ui` — image `provectuslabs/kafka-ui:latest` on port 8080 (optional but
    cheap; it makes "is the message actually there" a browser refresh instead of a
    CLI incantation, and it is genuinely useful during the demo).
  - **No Kibana.** Dropped from scope deliberately — kafka-ui plus the
    Streamlit dashboard already cover "is the data there / does it look
    right," and Kibana adds a service + port for no exercised use case in this
    project.
- **Dual listeners on Kafka — this is the critical part.** Configure two listeners
  so the same broker is reachable by both audiences:
  - `PLAINTEXT://kafka:9092` — advertised to other containers (Spark, kafka-ui)
  - `PLAINTEXT_HOST://localhost:29092` — advertised to processes on the host
    (your producer scripts run outside Docker during development)
  Set `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` for both, and
  `KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT`. Document in a comment that
  in-container clients use port 9092 and host clients use 29092.
- **Single-node broker settings** — set all three replication factors to `1`:
  `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR`,
  `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR`,
  `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR`. Without these the broker refuses to
  create internal topics and consumers hang with no useful error.
- **Healthchecks** on `kafka` (broker API versions responds) and `elasticsearch`
  (`/_cluster/health` returns yellow or green), so `depends_on: condition:
  service_healthy` works and startup ordering is deterministic.
- **Named volumes** for Kafka log dirs and Elasticsearch data, so `docker compose
  down` preserves state and `docker compose down -v` gives a clean slate.
- **`scripts/verify_stack.sh`** — a smoke test that curls Elasticsearch, curls
  kafka-ui's health endpoint, and lists Kafka topics from the host. Prints one
  PASS/FAIL line per service and exits non-zero if any failed.
- **`versions.md`** at the repo root recording the exact resolved image tags,
  Python version, and pinned pip packages. Every prompt given to an AI coding
  assistant in this repo should have this file pasted in as context.

## Non-goals
- No TLS, no authentication, no `xpack.security`. This is a local course project;
  security is explicitly disabled to save setup time. State this as a deliberate
  tradeoff in `versions.md`, not as an oversight.
- No multi-broker cluster, no rack awareness, no Kafka Connect.
- No Spark service in this compose file. Spark runs in local mode from a plain
  Python process (consumer half's decision); adding a master and worker is a
  possible week-two extension, not a day-one dependency.
- No resource limits beyond the Elasticsearch heap cap.

## Acceptance criteria
- On a clean machine: `docker compose down -v && docker compose up -d`, wait for
  healthchecks, then `bash scripts/verify_stack.sh` prints PASS for all services.
- From the **host**: `kafka-console-producer --bootstrap-server localhost:29092`
  successfully writes to a test topic, and `kafka-console-consumer
  --bootstrap-server localhost:29092 --from-beginning` reads it back.
- From **inside a container** (`docker compose exec kafka-ui sh` or any container
  on the network): the broker is reachable at `kafka:9092`.
- Elasticsearch responds to `curl localhost:9200/_cluster/health` with status
  `yellow` or `green` and requires no credentials.
- `docker compose restart` preserves previously written topics and messages;
  `docker compose down -v` removes them.
- `versions.md` lists every image tag with no `latest` except `kafka-ui`.

## Files
- `docker-compose.yml` (new)
- `scripts/verify_stack.sh` (new)
- `versions.md` (new)
- `.gitignore` (new — exclude `__pycache__/`, `.venv/`, `data/`)

## References
Ticket 0003 provisions topics against this broker. The dual-listener requirement
exists because host-run producer scripts (0004–0007) and container-run consumers
must both connect to the same broker.
