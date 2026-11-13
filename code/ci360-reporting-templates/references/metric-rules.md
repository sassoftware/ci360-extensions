# Metric and grain rules

## Define before calculating

For every measure, state:

1. Population: qualifying records after filters.
2. Numerator and denominator.
3. Deduplication key.
4. Reporting grain.
5. Time basis and timezone.
6. Null and late-arriving-event handling.

## Conventions

- Do not label a raw row count as people, contacts, sessions, sends, or journeys unless the selected key proves that meaning.
- Use `COUNT(DISTINCT key)` only when the business metric requires unique entities; explain the key.
- Use `NULLIF(denominator, 0)` for rates. State whether the result is a fraction or percentage.
- Keep delivery, engagement, conversion, suppression, exit, and journey-success populations separate unless the requested funnel explicitly relates them.
- Retain zero-activity dimension members only when required; use the join direction that matches that requirement.
- Resolve multiple opens/clicks explicitly: first, last, every event, or distinct recipient. Default to the earliest qualifying engagement only when calculating time-to-first-engagement and say so.
- Treat a lookback filter as a boundary on specified event timestamps, not automatically on every joined metadata table.
- For journey performance, report `success_nbr` and `success_val` as separate measures; do not infer a monetary unit for `success_val`.
- For Sankey paths, preserve ordered steps and aggregate equivalent paths by session count. Apply `path_cut_off` before producing edges.

## Fan-out checks

Before finalizing, identify the intended unique key for each input relation. If joins can multiply rows:

- aggregate or deduplicate the many-side first;
- count distinct business keys where appropriate; and
- include a validation query comparing pre-join and post-join populations.
