-- Define parameters used throughout the query.
-- The lookback timestamp limits the amount of email activity history processed.
WITH parameters AS (
    SELECT TIMESTAMP '2020-01-01 00:00:00' AS lookback
),

-- Aggregate email sends by task occurrence.
-- Pre-aggregating the detail data reduces the amount of data processed
-- by the subsequent joins.
email_send_subset AS (
    SELECT
        task_id,
        occurrence_id,
        COUNT(*) AS email_sends
    FROM udm.email_send
	CROSS JOIN parameters p
    WHERE email_send_dttm_tz > p.lookback
    GROUP BY
        task_id,
        occurrence_id
),


-- Aggregate email opt-outs by task occurrence.
email_optout_subset AS (
    SELECT
        task_id,
        occurrence_id,
        COUNT(*) AS email_optouts
    FROM udm.email_optout
	CROSS JOIN parameters p
    WHERE email_optout_dttm_tz > p.lookback
    GROUP BY
        task_id,
        occurrence_id
),


-- Aggregate email complaints by task occurrence.
email_complaint_subset AS (
    SELECT
        task_id,
        occurrence_id,
        COUNT(*) AS email_complaints
    FROM udm.email_complaint
	CROSS JOIN parameters p
    WHERE email_complaint_dttm_tz > p.lookback
    GROUP BY
        task_id,
        occurrence_id
)


-- Combine email sends with complaint and opt-out metrics
-- and enrich the results with task and occurrence metadata.
SELECT
    t.task_nm,
    t.task_cd,
    t.task_delivery_type_nm,
    COALESCE(o.occurrence_no, 0) AS occurrence_no,
    s.email_sends,
    COALESCE(c.email_complaints, 0) AS email_complaints,
    
    -- Calculate the complaint rate as the number of complaints
    -- divided by the number of email sends.
    -- NULLIF prevents division by zero.    
    CAST(COALESCE(c.email_complaints, 0) AS DECIMAL(18,6))
        / NULLIF(s.email_sends, 0) AS complaint_rate,

    COALESCE(op.email_optouts, 0) AS email_optouts,


    -- Calculate the opt-out rate as the number of opt-outs
    -- divided by the number of email sends.    
    CAST(COALESCE(op.email_optouts, 0) AS DECIMAL(18,6))
        / NULLIF(s.email_sends, 0) AS optout_rate,

    t.task_subtype_nm,
    o.end_tm AS occurrence_run_dttm
FROM email_send_subset s
INNER JOIN udm.md_task t ON s.task_id = t.task_id
LEFT JOIN udm.md_occurrence o ON s.occurrence_id = o.occurrence_id
LEFT JOIN email_optout_subset op ON s.occurrence_id = op.occurrence_id
LEFT JOIN email_complaint_subset c ON s.occurrence_id = c.occurrence_id

-- Show task occurrences with the largest number of complaints first,
-- followed by those with the largest number of opt-outs.
ORDER BY email_complaints DESC, email_optouts DESC;