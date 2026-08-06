"""Pull `EX-99.1` earnings press releases from 8-K filings and save as parquet.

Output conforms to the frozen `sec.text.v1` contract
(schemas/sec.text.v1.schema.json / schemas/README.md): one row per press
release, `section` always `press_release`, `chunk_index`/`chunk_total` always
`0`/`1` — chunking is not needed at this size.

Only `press_release` is produced. `risk_factors` and `mda` are reserved in the
schema but require splitting a 10-K by `Item N` heading, which is a real
parsing problem (see schemas/README.md) and out of scope here.

One file per ticker in `historical_data/sec.text.v1.historical/`, plus a
combined `all.parquet`, mirroring `fetch_historical_filings.py`.

Requires `SEC_IDENTITY` (a real name + email) in the environment: SEC requires
it as a User-Agent on every request and will block requests without one.
"""

import os
import re
import sys
import time
from datetime import datetime, timezone

import pandas as pd
from edgar import Company, set_identity

# Same universe as fetch_historical_filings.py.
TICKERS = [
    "NVDA", "AAPL", "MSFT", "AMZN", "GOOGL",
    "AVGO", "META", "TSLA", "BRK.B", "JPM",
]

OUT_DIR = "historical_data/sec.text.v1.historical"
FORM = "8-K"
SOURCE_DOCUMENT = "EX-99.1"

CONTRACT_COLUMNS = [
    "schema_version", "cik", "ticker", "accession_no", "form_type", "filed_date",
    "section", "source_document", "title", "text", "chunk_index", "chunk_total",
    "ingested_at",
]

_EXHIBIT_MARKER_RE = re.compile(r"^exhibit\s+99\.\d+$", re.IGNORECASE)


def normalize_text(raw: str) -> "str | None":
    """HTML is already stripped by the attachment reader; this collapses
    whitespace and drops the leading `Exhibit 99.1` marker line.

    Blank-line runs collapse to one, so the text never contains three or more
    consecutive newlines. Returns None if nothing usable remains.
    """
    lines = [line.strip() for line in raw.replace("\r\n", "\n").replace("\r", "\n").split("\n")]

    while lines and not lines[0]:
        lines.pop(0)
    if lines and _EXHIBIT_MARKER_RE.match(lines[0]):
        lines.pop(0)
    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()

    collapsed = []
    blank_run = 0
    for line in lines:
        if line == "":
            blank_run += 1
            if blank_run <= 1:
                collapsed.append(line)
        else:
            blank_run = 0
            collapsed.append(line)

    text = "\n".join(collapsed)
    return text or None


def extract_title(normalized_text: str) -> "str | None":
    """First non-empty line, for display only. Null rather than a truncated
    sentence if the line does not fit the schema's 300-char limit.
    """
    for line in normalized_text.split("\n"):
        line = line.strip()
        if not line:
            continue
        return line if len(line) <= 300 else None
    return None


def _fetch_text_with_retry(attachment, attempts: int = 3, backoff: float = 2.0) -> str:
    """SEC intermittently rate-limits with a throttling page that parses as
    garbage rather than raising cleanly. Retrying with backoff clears it; a
    single flaky filing should not take down the rest of the ticker's data.
    """
    last_exc = None
    for attempt in range(attempts):
        try:
            return attachment.text()
        except Exception as exc:  # noqa: BLE001 - anything means "retry", the caller logs it
            last_exc = exc
            time.sleep(backoff * (attempt + 1))
    raise last_exc


def press_release_row(cik: str, ticker: str, filing, ingested_at: str) -> "dict | None":
    attachments = [a for a in filing.attachments if a.document_type == SOURCE_DOCUMENT]
    if not attachments:
        return None

    raw = _fetch_text_with_retry(attachments[0])
    text = normalize_text(raw)
    if not text:
        return None

    return {
        "schema_version": 1,
        "cik": cik,
        "ticker": ticker,
        "accession_no": filing.accession_no,
        "form_type": filing.form,
        "filed_date": str(filing.filing_date)[:10],
        "section": "press_release",
        "source_document": SOURCE_DOCUMENT,
        "title": extract_title(text),
        "text": text,
        "chunk_index": 0,
        "chunk_total": 1,
        "ingested_at": ingested_at,
    }


def fetch_ticker(ticker: str, ingested_at: str) -> pd.DataFrame:
    company = Company(ticker)
    cik = f"{company.cik:010d}"

    filings = company.get_filings(form=FORM)
    if filings is None or len(filings) == 0:
        return pd.DataFrame(columns=CONTRACT_COLUMNS)

    rows = []
    for filing in filings:
        try:
            row = press_release_row(cik, ticker, filing, ingested_at)
        except Exception as exc:
            print(f"  {filing.accession_no}: SKIPPED ({exc})")
            continue
        if row is not None:
            rows.append(row)
        time.sleep(0.3)  # SEC rate limit is 10 req/s; each attachment fetch is its own request

    return pd.DataFrame(rows, columns=CONTRACT_COLUMNS)


def main() -> None:
    sec_identity = os.getenv("SEC_IDENTITY")
    if not sec_identity:
        sys.exit(
            "SEC_IDENTITY is not set. SEC requires a real name and email as a "
            "User-Agent on every EDGAR request and will block unidentified "
            "traffic. Set it, e.g.:\n"
            '  export SEC_IDENTITY="Your Name your.email@example.com"'
        )
    set_identity(sec_identity)

    os.makedirs(OUT_DIR, exist_ok=True)
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    frames = []
    skipped = []
    for ticker in TICKERS:
        print(f"Fetching {ticker}...")
        try:
            df = fetch_ticker(ticker, ingested_at)
        except Exception as exc:
            print(f"  SKIPPED: {exc}")
            skipped.append((ticker, str(exc)))
            continue
        out_path = os.path.join(OUT_DIR, f"{ticker.replace('.', '_')}.parquet")
        df.to_parquet(out_path, index=False)
        print(f"  {len(df)} press releases -> {out_path}")
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=CONTRACT_COLUMNS)
    combined_path = os.path.join(OUT_DIR, "all.parquet")
    combined.to_parquet(combined_path, index=False)
    print(f"Combined: {len(combined)} rows -> {combined_path}")
    if skipped:
        print(f"Skipped {len(skipped)} ticker(s): {skipped}")


if __name__ == "__main__":
    main()
