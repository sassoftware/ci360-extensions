# SQL dialect guide

The bundled templates were tested against PostgreSQL and CI 360 UDM schema version 22. Default to PostgreSQL unless the user names another platform.

## Portability checklist

| Area | PostgreSQL baseline | Adaptation requirement |
|---|---|---|
| Timestamp arithmetic | timestamp subtraction or `INTERVAL` | translate to the target platform's date-difference functions |
| Date truncation | `date_trunc` | map units and return types explicitly |
| Casts | `CAST(x AS type)` or `x::type` | prefer ANSI `CAST` for portability |
| Null handling | `COALESCE`, `NULLIF` | generally portable; verify result types |
| String aggregation | `string_agg` | map ordering, delimiter, and length behavior |
| Row limiting | `LIMIT` | use the platform's supported syntax |
| Identifiers | unquoted lowercase by default | quote only when required; preserve case rules |
| Boolean values | `TRUE`/`FALSE` | map to numeric or character flags if necessary |

## Adaptation rules

- Change syntax, not business semantics.
- Preserve event timestamp choice and inclusive/exclusive boundaries.
- State all changed expressions when producing a non-PostgreSQL version.
- Do not assume schema names, timezone conversion functions, or physical indexes.
- If the target platform is unknown, provide PostgreSQL SQL plus a short portability note rather than several speculative variants.
