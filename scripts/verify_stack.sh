#!/usr/bin/env bash
# Smoke test for the stack brought up by `docker compose up -d`.
# Prints one PASS/FAIL line per check and exits non-zero if anything failed.
#
# Usage: bash scripts/verify_stack.sh

set -uo pipefail

fail=0

check() {
    local name="$1"
    local cmd="$2"
    if eval "$cmd" >/dev/null 2>&1; then
        echo "PASS: $name"
    else
        echo "FAIL: $name"
        fail=1
    fi
}

# ---- .env --------------------------------------------------------------------
# Checked first: without it `docker compose up` fails on the env_file services,
# and the resulting error names the file but not the fix.
if [ -f .env ]; then
    echo "PASS: .env exists"
    set -a; . ./.env; set +a
else
    echo "FAIL: .env exists  (run: cp .env.example .env)"
    fail=1
fi

PRICES_TOPIC="${PRICES_TOPIC:-market.prices.v1}"
FILINGS_TOPIC="${FILINGS_TOPIC:-sec.filings.v1}"
TEXT_TOPIC="${TEXT_TOPIC:-sec.text.v1}"

# ---- services ----------------------------------------------------------------
check "elasticsearch reachable at localhost:9200" \
    "curl -sf http://localhost:9200/_cluster/health | grep -Eq '\"status\":\"(yellow|green)\"'"

check "kafka-ui reachable at localhost:8080" \
    "curl -sf http://localhost:8080/actuator/health"

check "kafka reachable from host at localhost:29092" \
    "docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list"

check "kafka reachable in-network at kafka:9092" \
    "docker compose exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9092"

# ---- topics ------------------------------------------------------------------
# Auto-create is disabled on the broker, so a missing topic here means the
# topic-init service did not run or did not succeed.
topics=$(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list 2>/dev/null)

for topic in "$PRICES_TOPIC" "$FILINGS_TOPIC" "$TEXT_TOPIC"; do
    check "topic $topic exists" "echo \"\$topics\" | grep -qx '$topic'"
done

if [ "$fail" -ne 0 ]; then
    echo
    echo "Something is down. Start with:  docker compose up -d"
    echo "Topics missing?                 docker compose run --rm topic-init"
fi

exit $fail
