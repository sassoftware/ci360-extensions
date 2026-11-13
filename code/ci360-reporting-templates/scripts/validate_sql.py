#!/usr/bin/env python3
"""Static safety and completeness checks for generated reporting SQL."""

from __future__ import annotations

import argparse
import pathlib
import re
import sys

FORBIDDEN = re.compile(
    r"\b(insert|update|delete|merge|drop|truncate|alter|grant|revoke|create)\b",
    re.IGNORECASE,
)
PLACEHOLDER = re.compile(r"(?:\{\{[^}]+\}\}|<[A-Z][^>]*>|\bTODO\b|\bTBD\b)", re.IGNORECASE)


def strip_comments_and_literals(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    sql = re.sub(r"--[^\n]*", " ", sql)
    sql = re.sub(r"'(?:''|[^'])*'", "''", sql)
    return sql


def validate(path: pathlib.Path) -> list[str]:
    text = path.read_text(encoding="utf-8-sig")
    scanned = strip_comments_and_literals(text)
    errors: list[str] = []

    if not re.search(r"\b(select|with)\b", scanned, re.IGNORECASE):
        errors.append("No SELECT or WITH statement found.")
    match = FORBIDDEN.search(scanned)
    if match:
        errors.append(f"Potentially mutating statement found: {match.group(1).upper()}.")
    if PLACEHOLDER.search(text):
        errors.append("Unresolved template marker found (TODO/TBD/{{...}}/<...>); use named SQL parameters instead.")
    if text.count("(") != text.count(")"):
        errors.append("Parenthesis counts do not match.")
    if not text.rstrip().endswith(";"):
        errors.append("SQL should end with a semicolon.")

    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", type=pathlib.Path)
    args = parser.parse_args()

    failed = False
    for path in args.files:
        if not path.is_file():
            print(f"FAIL {path}: file not found")
            failed = True
            continue
        errors = validate(path)
        if errors:
            failed = True
            print(f"FAIL {path}")
            for error in errors:
                print(f"  - {error}")
        else:
            print(f"PASS {path}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
