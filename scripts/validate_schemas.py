#!/usr/bin/env python3
"""
Validate every sample message in schemas/samples/ against its JSON Schema.

A sample named `<topic>.json` is validated against `schemas/<topic>.schema.json`.
Samples are one JSON message per line (kafka-console-producer format), so every
line is validated independently.

Exits 0 when everything validates, 1 otherwise. Run it before committing any
change to the message contract:

    python scripts/validate_schemas.py
"""
import json
import sys
from pathlib import Path

try:
    from jsonschema import Draft202012Validator
except ImportError:
    sys.exit(
        "jsonschema is not installed. Install it with:\n"
        "    pip install -r scripts/requirements.txt"
    )

SCHEMAS_DIR = Path(__file__).resolve().parent.parent / "schemas"
SAMPLES_DIR = SCHEMAS_DIR / "samples"


def describe(error):
    """Render a validation error as `field: message`, naming the offending field."""
    # error.absolute_path is the path to the failing value; for `additionalProperties`
    # and `required` the offending key lives in the message rather than the path.
    location = ".".join(str(part) for part in error.absolute_path) or "<root>"
    return f"{location}: {error.message}"


def validate_sample(sample_path):
    """Validate one sample file. Returns (messages_checked, [error strings])."""
    schema_path = SCHEMAS_DIR / f"{sample_path.stem}.schema.json"
    if not schema_path.exists():
        return 0, [f"no schema found at {schema_path.relative_to(SCHEMAS_DIR.parent)}"]

    validator = Draft202012Validator(json.loads(schema_path.read_text()))

    checked = 0
    errors = []
    for lineno, line in enumerate(sample_path.read_text().splitlines(), start=1):
        if not line.strip():
            continue
        checked += 1
        try:
            message = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append(f"line {lineno}: not valid JSON: {exc}")
            continue
        for error in sorted(validator.iter_errors(message), key=lambda e: list(e.absolute_path)):
            errors.append(f"line {lineno}: {describe(error)}")

    if checked == 0:
        errors.append("file contains no messages")
    return checked, errors


def main():
    samples = sorted(SAMPLES_DIR.glob("*.json"))
    if not samples:
        sys.exit(f"no sample messages found in {SAMPLES_DIR}")

    failed = False
    for sample_path in samples:
        checked, errors = validate_sample(sample_path)
        name = sample_path.relative_to(SCHEMAS_DIR.parent)
        if errors:
            failed = True
            print(f"FAIL {name}")
            for error in errors:
                print(f"       {error}")
        else:
            print(f"OK   {name} ({checked} message{'s' if checked != 1 else ''})")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
