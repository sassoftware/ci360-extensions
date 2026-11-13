# Template catalog

Select the closest template from `../templates/`. Read both its Markdown notes and SQL before adapting it.

| Reporting intent | Documentation | SQL | Typical grain or output |
|---|---|---|---|
| Task delivery, response, or conversion performance | `../task-performance.md` | `../templates/task_performance.sql` | task or occurrence with performance measures |
| Why people exited journeys | `../reasons-for-people-exit-journeys.md` | `../templates/reason_to_exit_journey.sql` | journey exit reason |
| Activity, form, page, visit, session, or custom-event audit | `../activity-audit.md` | `../templates/activity_audit.sql` | activity/event detail |
| Email sends, opt-outs, complaints, or deliverability | `../email-deliverability.md` | `../templates/email_deliverability.sql` | task/occurrence delivery measures |
| Journey suppressions and associated audience | `../journey-suppressions.md` | `../templates/journey_suppression.sql` | suppression event or reason |
| Which direct-contact tasks were executed | `../executed-tasks-listing.md` | `../templates/executed_tasks.sql` | executed task listing |
| Most-navigated business-process paths for a Sankey chart | `../sankey-diagram-most-navigated-paths.md` | `../templates/sankey_path_analysis.sql` | path step/source-target edge with session count |
| Time from email send to open or click | `../email-engagement-timing.md` | `../templates/email_engagement_timing.sql` | send/open/click timing by task or occurrence |
| Journey entries, suppressions, and success | `../journey-performance.md` | `../templates/journey_performance.sql` | journey or occurrence performance |

## Selection guidance

- Use task performance for campaign-task outcomes; use email deliverability when the focus is send, complaint, or opt-out events.
- Use email engagement timing when elapsed time to open or click is central.
- Use journey performance for aggregate entries and success; use exit reasons or suppressions for those specific populations.
- Use activity audit for traceable behavioral-event detail rather than campaign outcomes.
- Use executed tasks listing for inventory and governance questions, not performance metrics.
- Use Sankey path analysis only for ordered business-process navigation.
