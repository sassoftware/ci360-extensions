/*
Remove the string " TIMESTAMP " for SQLServer
*/
-- Form details
WITH parameters AS (
    select TIMESTAMP '2020-01-01 00:00:01' as p_begin_timestamp
        ,TIMESTAMP '2026-12-31 23:59:59' as p_end_timestamp
        ,'customer_id' as p_identifier_type
        ,'4278' as p_identifier_val
)
SELECT f.*
FROM udm.form_details f
CROSS JOIN parameters p 
INNER JOIN udm.session_details s
    ON f.session_id_hex = s.session_id_hex
INNER JOIN udm.identity_map im
    ON s.identity_id = im.source_identity_id
INNER JOIN udm.identity_attributes ia
    ON im.target_identity_id = ia.identity_id
   AND ia.identifier_type_id = p.p_identifier_type
WHERE 1 = 1
  AND f.load_dttm BETWEEN p.p_begin_timestamp
                    AND p.p_end_timestamp
  AND user_identifier_val = p.p_identifier_val;

-- Pages visited
WITH parameters AS (
    select TIMESTAMP '2020-01-01 00:00:01' as p_begin_timestamp
        ,TIMESTAMP '2026-12-31 23:59:59' as p_end_timestamp
        ,'customer_id' as p_identifier_type
        ,'4278' as p_identifier_val
)
SELECT f.*
FROM udm.page_details f
CROSS JOIN parameters p 
INNER JOIN udm.identity_map im
    ON f.identity_id = im.source_identity_id
INNER JOIN udm.identity_attributes ia
    ON im.target_identity_id = ia.identity_id
   AND ia.identifier_type_id = p.p_identifier_type
WHERE 1 = 1
  AND f.detail_dttm BETWEEN p_begin_timestamp
                      AND p.p_end_timestamp
  AND user_identifier_val = p.p_identifier_val;

-- Session details
WITH parameters AS (
    select TIMESTAMP '2020-01-01 00:00:01' as p_begin_timestamp
        ,TIMESTAMP '2026-12-31 23:59:59' as p_end_timestamp
        ,'customer_id' as p_identifier_type
        ,'4278' as p_identifier_val
)
SELECT f.*
FROM udm.session_details f
CROSS JOIN parameters p 
INNER JOIN udm.identity_map im
    ON f.identity_id = im.source_identity_id
INNER JOIN udm.identity_attributes ia
    ON im.target_identity_id = ia.identity_id
   AND ia.identifier_type_id = p.p_identifier_type  
WHERE 1 = 1
  AND f.session_start_dttm BETWEEN p_begin_timestamp
                             AND p.p_end_timestamp
  AND user_identifier_val = p.p_identifier_val;

-- Visit details
WITH parameters AS (
    select TIMESTAMP '2020-01-01 00:00:01' as p_begin_timestamp
        ,TIMESTAMP '2026-12-31 23:59:59' as p_end_timestamp
        ,'customer_id' as p_identifier_type
        ,'4278' as p_identifier_val
)
SELECT f.*
FROM udm.visit_details f
CROSS JOIN parameters p
INNER JOIN udm.identity_map im
    ON f.identity_id = im.source_identity_id
INNER JOIN udm.identity_attributes ia
    ON im.target_identity_id = ia.identity_id
   AND ia.identifier_type_id = p.p_identifier_type
WHERE 1 = 1
  AND f.visit_dttm BETWEEN p_begin_timestamp
                     AND p.p_end_timestamp
  AND user_identifier_val = p.p_identifier_val;

-- Custom events
WITH parameters AS (
    select TIMESTAMP '2020-01-01 00:00:01' as p_begin_timestamp
        ,TIMESTAMP '2026-12-31 23:59:59' as p_end_timestamp
        ,'customer_id' as p_identifier_type
        ,'4278' as p_identifier_val
)
SELECT f.*
FROM udm.custom_events f
CROSS JOIN parameters p
INNER JOIN udm.identity_map im
    ON f.identity_id = im.source_identity_id
INNER JOIN udm.identity_attributes ia
    ON im.target_identity_id = ia.identity_id
   AND ia.identifier_type_id = p.p_identifier_type
WHERE 1 = 1
  AND f.custom_event_dttm BETWEEN p_begin_timestamp
                            AND p.p_end_timestamp
  AND user_identifier_val = p.p_identifier_val;