-- Aggregate direct contact counts by task, occurrence, contact date,
-- and associated segment.
WITH parameters AS (
    select TIMESTAMP '2026-09-01 00:00:01' as p_begin_timestamp
        ,TIMESTAMP '2026-12-31 23:59:59' as p_end_timestamp
)
SELECT t.task_cd,
       t.task_nm,
	     mo.occurrence_no,
       max(dc.direct_contact_dttm) as direct_contact_dttm,
       s.segment_nm,
       COUNT(*) AS total_count,

       -- Count the distinct identities that were targeted.
       -- An identity is considered targeted when it is not assigned
       -- to an active control group.       
       COUNT(DISTINCT CASE
                        WHEN dc.control_active_flg = '0'
                          OR dc.control_group_flg  = '0'
                        THEN dc.identity_id
                        ELSE NULL
                      END) AS targeted_count,

       -- Count the distinct identities assigned to an active control group.
       -- These identities are held out from receiving the actual treatment.
       COUNT(DISTINCT CASE
                        WHEN dc.control_active_flg = '1'
                         AND dc.control_group_flg  = '1'
                        THEN dc.identity_id
                        ELSE NULL
                      END) AS holdout_count
FROM udm.md_task t
CROSS JOIN parameters p
INNER JOIN udm.direct_contact dc
    ON  dc.task_id         = t.task_id
    AND dc.task_version_id = t.task_version_id
INNER JOIN udm.md_occurrence mo
	ON mo.occurrence_id = dc.occurrence_id    
INNER JOIN udm.md_task_x_segment ts
    ON t.task_id = ts.task_id
INNER JOIN udm.md_segment s
    ON s.segment_id = ts.segment_id
WHERE dc.direct_contact_dttm BETWEEN p.p_begin_timestamp
                    AND p.p_end_timestamp 
GROUP BY t.task_cd,
         t.task_nm,
         mo.occurrence_no,
         s.segment_nm;