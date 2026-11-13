-- Define parameters used throughout the query.
-- The lookback timestamp limits the amount of email engagement history processed.
WITH parameters AS (
    SELECT TIMESTAMP '2025-01-01 00:00:00' AS lookback
),
 
-- Determine the first genuine email open for each email imprint.
first_open AS (
    SELECT
        task_id,
        occurrence_id,
        imprint_id,
        MIN(email_open_dttm_tz) AS first_open_dttm_tz
    FROM udm.email_open
    WHERE email_open_dttm_tz > (select lookback from parameters)
      AND prefetched_flg <> '1' -- exclude email-opens triggered by the mail provider
    GROUP BY task_id, occurrence_id, imprint_id
),
 

-- Determine the first email click for each email imprint.
first_click AS (
     SELECT
        task_id,
        occurrence_id,
        imprint_id,
        MIN(email_click_dttm_tz) AS first_click_dttm_tz
    FROM udm.email_click
    WHERE email_click_dttm_tz > (select lookback from parameters)
    GROUP BY task_id, occurrence_id, imprint_id
),
 
-- Combine email sends with their first open and first click.
-- Calculate both the hour of engagement and the elapsed time
-- between the email send and the first engagement.
email_engagement AS (
    SELECT
        s.task_id,
        s.occurrence_id,

        -- Hour of day when the recipient first opened the email.
        EXTRACT(HOUR FROM o.first_open_dttm_tz) AS open_hour,

        -- Hour of day when the recipient first clicked the email.
        EXTRACT(HOUR FROM c.first_click_dttm_tz) AS click_hour,

        -- Number of hours between email delivery and the first open.
        EXTRACT(EPOCH FROM (o.first_open_dttm_tz - s.email_send_dttm_tz))/3600 AS hours_to_open,

        -- Number of hours between email delivery and the first click.
        EXTRACT(EPOCH FROM (c.first_click_dttm_tz - s.email_send_dttm_tz))/3600 AS hours_to_click

    FROM udm.email_send s
    LEFT JOIN first_open o ON s.imprint_id = o.imprint_id
    LEFT JOIN first_click c ON s.imprint_id = c.imprint_id
    WHERE s.email_send_dttm_tz > (select lookback from parameters)

    -- Retain only emails that have at least one open or click.
	  and (first_open_dttm_tz is not NULL or first_click_dttm_tz is not NULL)
)

-- Aggregate email engagement metrics by task occurrence.
SELECT
    t.task_nm,
    t.task_cd,
    COALESCE(o.occurrence_no, 0) AS occurrence_no,
    COUNT(e.open_hour) AS email_opens,
    COUNT(e.click_hour) AS email_clicks,
 
    -- Most common hour of day for the first email open.
    MODE() WITHIN GROUP (ORDER BY e.open_hour) AS most_frequent_open_hour,

    -- Median number of hours between email delivery and the first open
    ROUND(CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY e.hours_to_open) AS NUMERIC), 2) AS median_hours_to_open,

    -- Most common hour of day for the first email click.
    MODE() WITHIN GROUP (ORDER BY e.click_hour) AS most_frequent_click_hour,

    -- Median number of hours between email delivery and the first click.
    ROUND(CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY e.hours_to_click) AS NUMERIC), 2) AS median_hours_to_click,
    o.end_tm AS occurrence_run_dttm
FROM email_engagement e
LEFT JOIN udm.md_task t ON e.task_id = t.task_id
LEFT JOIN udm.md_occurrence o ON e.occurrence_id = o.occurrence_id

-- Limit the results to bulk email tasks.
WHERE t.task_delivery_type_nm = 'bulk'
GROUP BY
    t.task_nm,
    t.task_cd,
    t.task_delivery_type_nm,
    o.occurrence_no,
    o.end_tm
ORDER BY email_opens DESC