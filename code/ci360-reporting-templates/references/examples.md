# Examples

## Task performance

**Request:** Show task performance since `2026-10-01`, grouped by task and occurrence, including conversion count and rate.

**Use:** `../templates/task_performance.sql`. Confirm the conversion key and rate denominator, preserve task/occurrence grain, and return SQL plus KPI/table guidance.

## Email engagement timing

**Request:** For email task `:task_id`, calculate time to first open and first click after each send during the last 30 days.

**Use:** `../templates/email_engagement_timing.sql`. Resolve multiple engagement events to the earliest qualifying event and state timezone and timestamp-boundary assumptions.

## Journey exits

**Request:** Explain why contacts exited journey `:journey_id` by reason and occurrence.

**Use:** `../templates/reason_to_exit_journey.sql`. Define whether the measure counts exit events or distinct contacts and guard against metadata join fan-out.

## Sankey paths

**Request:** Build source-target edges for business process `:process_name`, lookback `:lookback_timestamp`, path cutoff `20`.

**Use:** `../templates/sankey_path_analysis.sql`. Preserve path order, apply the cutoff, aggregate equivalent paths by sessions, and recommend a Sankey visual.

## Missing inputs

If a request says only “give me journey performance,” select `../templates/journey_performance.sql`, keep placeholders such as `:lookback_timestamp`, and explicitly list the unresolved journey filter, timezone, and desired grain instead of inventing values.
