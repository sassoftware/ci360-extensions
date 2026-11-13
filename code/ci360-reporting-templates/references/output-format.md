# Output format

Return sections in this order. Omit a section only when it truly does not apply.

## Interpretation

One short paragraph stating the population, metric, grain, time range, and chosen template.

## Parameters

| Parameter | Value or placeholder | Purpose |
|---|---|---|

Use named placeholders for unresolved values.

## SQL

Provide one complete, read-only SQL block. Add comments only where they explain a non-obvious parameter, deduplication, metric, or dialect choice.

## Metric and grain

List each output measure with numerator, denominator if applicable, deduplication key, and reporting grain.

## Report guidance

Recommend an appropriate table, KPI, trend, funnel, distribution, or Sankey visual. Name useful dimensions and sorting behavior.

## Assumptions

List only material assumptions, including UDM version, dialect, timezone, event boundary, missing parameter, and unverified schema fields.

## Validation checks

Provide focused checks for source population, duplicate/fan-out risk, nulls, boundary timestamps, reconciliation to a known sample, and dialect compatibility.
