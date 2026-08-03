---
id: 0011
title: Dagster orchestration for the interval Spark run (stretch)
status: todo
layer: orchestration
priority: P3
depends_on: [0007, 0009]
---

## Goal
Presentation day needs Kafka pre-loaded with historical data, then topped up
with a slow trickle of live/simulated data while Spark recomputes features +
KMeans + sentiment on an interval and the dashboard picks up the new results.
The minimal version of that is a shell loop re-running `docker compose run
--rm spark` every N seconds. This ticket is the alternative: wrap the same
recompute step in Dagster so the pipeline is scheduled and observable through
a UI instead of a loop script.

**This is explicitly a backpocket / architecture-sophistication item, not on
the critical path.** The professor is expected to reward architecture
sophistication, so this is worth having ready if there's time left after the
core pipeline (backfill → replay → interval Spark run → live dashboard) is
verified end-to-end. If it isn't, present the loop-script version and
describe Dagster as a considered next step — that is a defensible position,
not an excuse (same framing 0008 uses for the live WebSocket).

## Scope
- A Dagster job/schedule that wraps the existing `spark/pipeline.py` batch
  run (or its containerized `docker compose run --rm spark` invocation) on a
  fixed interval, rather than rewriting the Spark logic itself.
- Dagster UI reachable during the demo, showing run history / success-failure
  status for each interval tick — this is the actual payoff over a bare loop
  script (visible orchestration, not just recomputation).
- Decide and document whether each run re-clusters from scratch (KMeans
  centroids can drift run to run) or only re-scores new days against fixed
  centroids — same open question already flagged in the discussion that
  spawned this ticket, needs an answer before this is buildable either way.

## Non-goals
- No rewrite of `spark/pipeline.py` to Spark Structured Streaming — that's a
  separate, larger change and not what this ticket is about.
- No Dagster asset catalog / partitioning sophistication beyond what's needed
  to demo a scheduled run. This is a orchestration-layer demo, not a Dagster
  showcase.
- Not required for `0009`'s cold-start/demo runbook to be considered done —
  the runbook's baseline path is the loop-script version.

## Acceptance criteria
- Kafka is pre-loaded with historical data and receiving a slow live/simulated
  trickle (via 0007's replay producer or 0008's live producer) while this is
  running.
- A Dagster schedule triggers the Spark recompute step on an interval without
  manual re-running, and the Dagster UI shows the run history live during a
  rehearsal.
- The dashboard reflects each interval's new results after it completes.
- Whatever the recluster-vs-rescore decision was, it's written down here or in
  `docs/DEMO.md`, not decided live during the demo.

## Files
- New: a `orchestration/` (or similar) folder with the Dagster job/schedule
  definition.
- `docker-compose.yml` — add a Dagster service if it needs to run
  containerized alongside the rest of the stack.
- `docs/DEMO.md` — note the orchestration mode chosen for the actual
  presentation slot, same as 0009's symbol-mode decision for 0008.

## References
Spawned from a discussion on interval Spark recomputation for the demo (see
`so_far.md` for the pipeline's current batch-only state). Mirrors 0008's
"cuttable, present the fallback as a deliberate choice" framing.
