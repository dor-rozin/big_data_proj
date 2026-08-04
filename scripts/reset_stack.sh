#!/usr/bin/env bash
# Wipe the stack back to a clean slate: stop everything, remove the Kafka and
# Elasticsearch volumes, bring it back up with empty topics.
#
# Kafka and Elasticsearch data persist across a plain `docker compose down` by
# design (ticket 0002) -- a routine restart shouldn't silently lose a demo's
# data. This script is the explicit, one-command way to ask for the opposite:
# a genuinely empty broker, e.g. before a fresh backfill or to reproduce a bug
# from a known-clean state.
#
# Usage: bash scripts/reset_stack.sh [--seed]
#   --seed   also run the backfill producer once the stack is back up, so you
#            land on the same "looks pre-populated" state a normal start gives.

set -euo pipefail

echo "==> Stopping the stack and removing volumes (kafka-data, es-data)..."
docker compose down -v

echo "==> Starting fresh..."
docker compose up -d

echo "==> Waiting for healthchecks..."
until [ "$(docker compose ps kafka --format '{{.Health}}' 2>/dev/null)" = "healthy" ] \
   && [ "$(docker compose ps elasticsearch --format '{{.Health}}' 2>/dev/null)" = "healthy" ]; do
    sleep 2
done

bash scripts/verify_stack.sh

if [ "${1-}" = "--seed" ]; then
    echo "==> Seeding a year of history (--mode backfill)..."
    docker compose run --rm producer
    echo "==> Done. Kafka holds a fresh year of backfilled history."
else
    echo "==> Done. Kafka is empty -- run 'docker compose run --rm producer' to seed it."
fi
