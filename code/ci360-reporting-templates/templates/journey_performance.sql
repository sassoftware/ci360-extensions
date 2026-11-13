-- Define parameters used throughout the query.
-- The lookback timestamp limits the amount of journey success history processed.
WITH parameters AS (
    SELECT
        TIMESTAMP '2026-01-01 00:00:00' AS lookback
)

-- Combine journey occurrence information with aggregated success metrics
-- and enrich the results with purpose and journey metadata.
SELECT
    p.purpose_nm,
    j.journey_nm,
    o.journey_occurrence_num,
    o.num_of_contacts_entered,
    o.num_of_contacts_suppressed,
    s.success_val,
    COALESCE(s.success_nbr, 0) AS success_nbr
FROM udm.md_journey j
INNER JOIN udm.md_purpose p ON j.purpose_id = p.purpose_id
INNER JOIN udm.md_journey_occurrence o ON j.journey_id = o.journey_id

-- Aggregate success events by journey occurrence before joining them
-- to the journey occurrence metadata.
INNER JOIN (
        SELECT
            journey_occurrence_id,
            SUM(success_val) AS success_val,
            COUNT(event_id) AS success_nbr
        FROM udm.journey_success
        WHERE success_dttm  > (select lookback from parameters)
        GROUP BY journey_occurrence_id
    ) s
    ON o.journey_occurrence_id = s.journey_occurrence_id
    
ORDER BY
    p.purpose_nm,
    j.journey_nm,
    o.journey_occurrence_num