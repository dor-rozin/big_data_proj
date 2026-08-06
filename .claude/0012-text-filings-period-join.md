---
id: 0012
title: Join sec.text.v1 to sec.filings.v1 by fiscal period (contract change)
status: todo
layer: producer
priority: P2
depends_on: [0001, 0010]
---

## Goal
Give the two SEC topics a join that is *semantically* correct rather than
merely approximate. Today the only working method is `cik` plus a `filed_date`
window, which tops out around 61% on earnings releases and is a proxy for the
thing we actually mean: **"the 8-K announcing Q3 results" belongs with "the 10-Q
for Q3."** That relationship is exact. Matching it by date proximity is not.

## Background — why the obvious join does not exist
`schemas/README.md` advertised an `accession_no` join between the two topics
until 2026-08-06. It matched **zero rows and always would**: an accession number
identifies one filing, `sec.filings.v1` carries `10-K`/`10-Q`, `sec.text.v1`
carries the `EX-99.1` of an `8-K`. Different documents, different days.

The contract has been corrected to document `cik` + date window instead, with
measured coverage:

| window | earnings-like releases | all text documents |
|---|---|---|
| exact | 20% | 11% |
| ±1 day | 54% | 30% |
| ±3 days | 61% | 35% |

The ±1-day cliff is caused by the property that makes this data valuable: the
earnings 8-K lands the day *before* the matching 10-Q, so it is dated to the day
the stock reacts.

**Do not "fix" this by adding `8-K` to `sec.filings.v1`.** That was investigated
and rejected — only ~2% of the 8-Ks we hold text for carry any XBRL facts
(AAPL 2/116, MSFT 3/190, JPM 0/322), so it would append ~1,324 rows that are
~98% null, each needing a fabricated `fiscal_period` because that field is a
required non-nullable enum and the XBRL `fp` source is empty. See the 2026-08-06
entry in `so_far.md` for the full measurement.

## Scope
- Add a **nullable** `fiscal_period` (`FY`/`Q1`–`Q4`) and **nullable**
  `period_end` to `sec.text.v1`. Nullable is essential: most 8-Ks are not tied
  to a period at all, and a non-nullable enum here is exactly the trap that
  makes the filings side unable to represent 8-Ks honestly.
- Derive them from the press release where it can be done **without guessing** —
  the parent 8-K's XBRL `dei` period tags where present, otherwise the period
  stated in the release. If neither resolves, emit `null`. A wrong period is
  worse than a missing one.
- Update `schemas/sec.text.v1.schema.json`, its field table in
  `schemas/README.md`, `spark/schemas.py`'s `TEXT_SCHEMA`, and the Elasticsearch
  mapping for the text index.
- Re-fetch / re-project the snapshot so existing rows carry the new fields.

## Non-goals
- **No change to `sec.filings.v1`.** It stays `10-K`/`10-Q`.
- **No reinstating the `accession_no` join.** It cannot work; the corrected
  contract text should not be softened back.
- **No dropping the date-window method.** It stays documented as the fallback
  for the rows where the period fields are null.

## Acceptance criteria
- A press release with a resolvable period joins to the periodic filing covering
  the same `(cik, fiscal_period, period_end)`, exactly, with no date tolerance.
- Coverage of that exact join, measured and written into `schemas/README.md`,
  beats the current ±3-day window's 61% on earnings-like releases.
- A press release with no resolvable period emits `null` for both fields and is
  still produced — never dropped, never guessed.
- `scripts/validate_schemas.py` passes, and the sample message in
  `schemas/samples/sec.text.v1.json` is regenerated to carry the new fields.

## This is a real contract change
Unlike the 2026-08-06 correction — which changed prose only, no field added or
retyped — this adds fields to a frozen topic. Per `CLAUDE.md` that is "a
conversation with the whole team, not a commit": it moves `spark/schemas.py` and
the Elasticsearch mapping, so Dor and Person C both need to agree before it
lands.

## References
`schemas/README.md` § "Do not join these two topics on `accession_no`" for the
measurements and the rejected alternative. Ticket 0010 for the producer this
extends.
