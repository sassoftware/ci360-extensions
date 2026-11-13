---
name: ci360-udm-reporting
description: generate and adapt sas customer intelligence 360 unified data model reporting sql from supplied business questions and sql parameters. use for task performance, email deliverability or engagement, journey performance or exits, suppressions, activity audits, executed tasks, and sankey path analysis. produce sql plus metric, grain, visualization, assumption, and validation guidance without executing a live database query.
---

# CI 360 UDM reporting

Translate a reporting question and available parameters into reviewable SQL and report guidance by adapting the bundled CI 360 UDM schema v22 templates.

## Inputs

Collect or infer only when safe:

- Business question and desired reporting grain.
- SQL parameters such as lookback timestamp, task or journey identifiers, business process name, path cutoff, schema name, and timezone.
- Target SQL dialect. Default to PostgreSQL when omitted.
- Expected output fields or visualization, if specified.

Do not require a live connector. Do not claim that SQL was executed.

## Workflow

1. Restate the requested metric, population, grain, time window, and filters.
2. Read `references/template-catalog.md` and select the closest template. For a compound question, combine the smallest compatible set and explain each join.
3. Read the selected Markdown documentation and corresponding SQL under `templates/` before adapting it.
4. Preserve UDM table and column names from the source template. Never invent a UDM field. Mark any requested field not established by the templates as a schema-validation requirement.
5. Substitute user parameters. If a required parameter is missing, use a conspicuous named placeholder such as `:lookback_timestamp` and list it under assumptions; never silently guess a business value.
6. Apply the metric and grain rules in `references/metric-rules.md` and dialect rules in `references/dialect-guide.md`.
7. Keep filters sargable where practical, guard divisions with `NULLIF`, make null handling explicit, and prevent accidental fan-out at one-to-many joins.
8. Run `python scripts/validate_sql.py <sql-file>` on SQL saved during the task. Treat this as static validation only.
9. Return the result using `references/output-format.md`.

## Template rules

- Prefer adapting a bundled query over rewriting it from scratch.
- Preserve event semantics and the template's relationship paths.
- State whether counts represent rows, events, contacts, sessions, occurrences, or distinct entities.
- Distinguish send-level, contact-level, task-level, journey-level, session-level, and path-level output.
- Keep conversion and success measures separate from delivery and engagement measures.
- For Sankey output, preserve source-to-target step order, path cutoff behavior, and session aggregation.
- For audit or listing requests, avoid aggregating away identifiers the user needs for traceability.

## Safety and validation

- Generate read-only reporting SQL. Do not emit `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `DROP`, `TRUNCATE`, `ALTER`, `GRANT`, or `REVOKE` unless the user explicitly requests database administration rather than reporting.
- Do not expose credentials or connection strings.
- Flag dialect-sensitive date functions, casts, interval syntax, identifier quoting, and string aggregation.
- Recommend validating row counts, duplicate keys, null rates, time boundaries, and a small known sample before production use.
- If templates conflict with the user's deployed schema version, present the mismatch and the exact objects requiring confirmation.

## References

- Use `references/template-catalog.md` to choose a template.
- Use `references/metric-rules.md` for metric and grain conventions.
- Use `references/dialect-guide.md` for portability guidance.
- Use `references/output-format.md` for the response structure.
- Use `references/examples.md` for representative request patterns.
- Use `README.md` and the paired Markdown/SQL files as the authoritative bundled examples.
