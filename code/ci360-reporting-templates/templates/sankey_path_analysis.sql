/*==============================================================================
  Transform CI 360 UDM business process details into a structure suitable
  for Path Analysis / Sankey visualization.

  PostgreSQL version of the original SAS program.
==============================================================================*/

WITH parameters AS (

    /* -------------------------------------------------------------------------
       Parameters

       lookback:
         Only process events loaded from this timestamp onward are included.

       path_cut_off:
         Business process paths with this number of steps or more are excluded.
       ------------------------------------------------------------------------- */
    SELECT
        TIMESTAMP '2026-09-05 20:00:00' AS lookback,
        20 AS path_cut_off
),

business_process_step_ids AS (

    /* -------------------------------------------------------------------------
       Create a numeric ID for every distinct business process step.

       The original SAS program used _N_ after sorting by process and step.
       ROW_NUMBER() provides the equivalent functionality in SQL.
       ------------------------------------------------------------------------- */
    SELECT
        process_nm,
        process_step_nm,
        ROW_NUMBER() OVER (
            ORDER BY process_nm, process_step_nm
        ) AS step_id
    FROM (
        SELECT DISTINCT
            process_nm,
            process_step_nm
        FROM udm.business_process_details
        WHERE load_dttm >= (
            SELECT lookback
            FROM parameters
        )
    ) distinct_steps
),

business_process_detail AS (

    /* -------------------------------------------------------------------------
       Retrieve the business process events and attach the generated step_id.

       LAG(step_id) identifies the previous step in the same process instance.
       This is later used to identify consecutive duplicate process steps.

       event_row_no identifies the first event in each process instance.
       ------------------------------------------------------------------------- */
    SELECT
        b.identity_id,
        b.session_id_hex,
        b.process_nm,
        b.process_step_nm,
        b.process_dttm_tz AS event_dttm,
        b.attribute1_txt,
        b.attribute2_txt,
        b.step_order_no,
        b.process_instance_no,
        b.is_completion_flg,
        b.is_start_flg,
        s.step_id,

        ROW_NUMBER() OVER (
            PARTITION BY
                b.session_id_hex,
                b.process_nm,
                b.process_instance_no
            ORDER BY
                b.process_dttm_tz,
                b.step_order_no
        ) AS event_row_no,

        LAG(s.step_id) OVER (
            PARTITION BY
                b.session_id_hex,
                b.process_nm,
                b.process_instance_no
            ORDER BY
                b.process_dttm_tz,
                b.step_order_no
        ) AS previous_step_id

    FROM udm.business_process_details b

    INNER JOIN business_process_step_ids s
        ON b.process_nm = s.process_nm
       AND b.process_step_nm = s.process_step_nm

    WHERE b.load_dttm >= (
        SELECT lookback
        FROM parameters
    )
),

business_process_detail_marked AS (

    /* -------------------------------------------------------------------------
       Mark changes between consecutive process steps.

       new_step_flg = 1:
         The current step differs from the previous step.

       sequence_no:
         Creates a zero-based sequence number.

         Example:
             Step A -> sequence 0
             Step A -> sequence 0
             Step B -> sequence 1
             Step B -> sequence 1
             Step C -> sequence 2

       This corresponds to the SAS DATA step that maintained
       last_process_step_nm and last_sequence_no.
       ------------------------------------------------------------------------- */
    SELECT
        d.*,

        CASE
            WHEN previous_step_id = step_id THEN 0
            ELSE 1
        END AS new_step_flg,

        SUM(
            CASE
                WHEN event_row_no = 1 THEN 0
                WHEN previous_step_id = step_id THEN 0
                ELSE 1
            END
        ) OVER (
            PARTITION BY
                session_id_hex,
                process_nm,
                process_instance_no
            ORDER BY
                event_dttm,
                step_order_no
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS sequence_no

    FROM business_process_detail d
),

business_path_ids AS (

    /* -------------------------------------------------------------------------
       Construct path_id.

       path_id contains every process step, including consecutive duplicates.

       Example:
           Step IDs: 01, 01, 04, 07
           path_id:  01010407

       PostgreSQL string_agg() replaces SAS CATS() accumulation.
       LPAD() replaces the SAS Z2. format.
       ------------------------------------------------------------------------- */
    SELECT
        session_id_hex,
        process_nm,
        process_instance_no,

        string_agg(
            LPAD(step_id::text, 2, '0'),
            ''
            ORDER BY event_dttm, step_order_no
        ) AS path_id

    FROM business_process_detail_marked

    GROUP BY
        session_id_hex,
        process_nm,
        process_instance_no
),

business_dedup_path_ids AS (

    /* -------------------------------------------------------------------------
       Construct dedup_path_id.

       Consecutive occurrences of the same process step are removed.

       Example:
           Step IDs:       01, 01, 04, 04, 07
           path_id:        0101040407
           dedup_path_id:  010407

       dedup_step_count is used to apply the path_cut_off parameter.
       ------------------------------------------------------------------------- */
    SELECT
        session_id_hex,
        process_nm,
        process_instance_no,

        string_agg(
            LPAD(step_id::text, 2, '0'),
            ''
            ORDER BY event_dttm, step_order_no
        ) AS dedup_path_id,

        COUNT(*) AS dedup_step_count

    FROM business_process_detail_marked

    WHERE new_step_flg = 1

    GROUP BY
        session_id_hex,
        process_nm,
        process_instance_no
),

business_path_ids_dedup_un AS (

    /* -------------------------------------------------------------------------
       Add path_id and dedup_path_id back to every process event.

       Also remove process instances whose deduplicated path contains
       path_cut_off steps or more.
       ------------------------------------------------------------------------- */
    SELECT
        d.identity_id,
        d.session_id_hex,
        d.process_nm,
        d.process_step_nm,
        d.event_dttm,
        d.attribute1_txt,
        d.attribute2_txt,
        d.step_order_no,
        d.process_instance_no,
        d.is_completion_flg,
        d.is_start_flg,
        d.step_id,
        p.path_id,
        dp.dedup_path_id,
        d.sequence_no

    FROM business_process_detail_marked d

    INNER JOIN business_path_ids p
        ON d.session_id_hex = p.session_id_hex
       AND d.process_nm = p.process_nm
       AND d.process_instance_no = p.process_instance_no

    INNER JOIN business_dedup_path_ids dp
        ON d.session_id_hex = dp.session_id_hex
       AND d.process_nm = dp.process_nm
       AND d.process_instance_no = dp.process_instance_no

    CROSS JOIN parameters prm

    WHERE dp.dedup_step_count < prm.path_cut_off
),

agg_business_process_detail AS (

    /* -------------------------------------------------------------------------
       Aggregate identical paths.

       This corresponds to agg_Bus_process_detail in the original SAS code.

       Note that COUNT(*) reproduces the original SAS logic. Despite the column
       name no_of_sessions, this actually counts rows in each group.
       ------------------------------------------------------------------------- */
    SELECT
        process_nm,
        dedup_path_id,
        path_id,
        sequence_no,
        process_step_nm,
        COUNT(*) AS no_of_sessions

    FROM business_path_ids_dedup_un

    GROUP BY
        process_nm,
        dedup_path_id,
        path_id,
        sequence_no,
        process_step_nm
),

agg_business_process_detail_sankey AS (

    /* -------------------------------------------------------------------------
       Aggregate again using dedup_path_id.

       This produces the final simplified data structure for the
       Path Analysis / Sankey visualization.
       ------------------------------------------------------------------------- */
    SELECT
        process_nm,
        dedup_path_id,
        sequence_no,
        process_step_nm,
        SUM(no_of_sessions) AS no_of_sessions

    FROM agg_business_process_detail

    GROUP BY
        process_nm,
        dedup_path_id,
        sequence_no,
        process_step_nm
)

/* -------------------------------------------------------------------------
   Final result
   ------------------------------------------------------------------------- */
SELECT
    process_nm,
    dedup_path_id,
    sequence_no,
    process_step_nm,
    no_of_sessions
FROM agg_business_process_detail_sankey
ORDER BY
    process_nm,
    dedup_path_id,
    sequence_no,
    process_step_nm