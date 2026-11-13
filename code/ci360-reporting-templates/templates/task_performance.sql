-- Define parameters used throughout the query.
-- The lookback timestamp limits the amount of historical data processed.
-- ADAPT TIMESTAMP VALUE TO DATABASE SQL DIALECT
WITH parameters AS (
    SELECT
        TIMESTAMP '2026-01-01 00:00:00' AS lookback
),

-- Select only contacts within the configured lookback period.
-- if the data is partitioned on load_dttm use that as your filter
contact_history_subset AS (
    SELECT
        contact_id,
        identity_id,
        contact_dttm_tz,
        task_id,
        occurrence_id,
        response_tracking_cd
    FROM udm.contact_history
    WHERE contact_dttm_tz > (select lookback from parameters)

),

-- Combine contacts with their associated milestone and conversion events.
-- Contacts without matching events are retained and assigned zero counts.
conversion_milestone_subset AS (
    SELECT
        event_id,
        identity_id,
        conversion_milestone_dttm_tz,
        response_tracking_cd,
        CASE
            WHEN event_nm = 'milestone' THEN 1
            ELSE 0
        END AS milestone_cnt,
        CASE
            WHEN event_nm = 'conversion' THEN 1
            ELSE 0
        END AS conversion_cnt
    FROM udm.conversion_milestone cm
    WHERE conversion_milestone_dttm_tz > (select lookback from parameters)
),


-- Combine contacts with their associated milestone and conversion events.
-- The response tracking code links related events.
-- Contacts without matching events are retained and assigned zero counts.
-- Aggregate interactions by task occurrence, response tracking code, and identity.
-- MAX on the milestone/conversion indicators identifies whether the identity
-- had at least one milestone or conversion for the interaction.
-- SUM retains the total number of milestone and conversion events.
interaction_by_identity AS (
    SELECT
        ch.task_id,
        ch.occurrence_id,
        ch.response_tracking_cd,
        ch.identity_id,
        MAX(ch.contact_dttm_tz) AS most_recent_contact_dttm,
        COUNT(DISTINCT ch.contact_id) AS contacts,
        MAX(COALESCE(cm.milestone_cnt,0)) AS unique_milestones,
        MAX(COALESCE(cm.conversion_cnt,0)) AS unique_conversions,
        SUM(COALESCE(cm.milestone_cnt,0)) AS milestones,
        SUM(COALESCE(cm.conversion_cnt,0)) AS conversions
    FROM contact_history_subset ch
    LEFT JOIN conversion_milestone_subset cm
        ON ch.identity_id = cm.identity_id
       AND ch.response_tracking_cd = cm.response_tracking_cd
    GROUP BY
        ch.task_id,
        ch.occurrence_id,
        ch.response_tracking_cd,
        ch.identity_id
),

-- Aggregate identity-level interactions to the task occurrence level.
-- This reduces the data to one row per task occurrence and response tracking code.
interaction_by_run AS (
    SELECT
        task_id,
        occurrence_id,
        response_tracking_cd,
        MAX(most_recent_contact_dttm) AS most_recent_contact_dttm,
        SUM(contacts) AS contacts,
        SUM(unique_milestones) AS unique_milestones,
        SUM(unique_conversions) AS unique_conversions,
        SUM(conversions) AS conversions,
        SUM(milestones) AS milestones
    FROM interaction_by_identity
    GROUP BY
        task_id,
        occurrence_id,
        response_tracking_cd
)

-- Enrich the aggregated interaction data with task and occurrence metadata.
SELECT
    i.most_recent_contact_dttm,
    t.channel_nm,
    t.task_delivery_type_nm,
    t.task_subtype_nm,
    t.task_nm,
    t.task_cd,
    o.occurrence_no,
    o.end_tm AS occurrence_run_dttm,
    i.contacts,
    i.unique_milestones,
    i.unique_conversions,
    i.conversions,
    i.milestones,
    t.task_type_nm,
    t.task_status_cd,
    t.task_desc,
    t.owner_nm,
    t.task_id,
    i.response_tracking_cd,
    i.occurrence_id
FROM udm.md_task t
INNER JOIN interaction_by_run i
    ON i.task_id = t.task_id
LEFT JOIN udm.md_occurrence o
    ON i.occurrence_id = o.occurrence_id
ORDER BY
    most_recent_contact_dttm DESC