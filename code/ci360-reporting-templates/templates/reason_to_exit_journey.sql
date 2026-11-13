-- Define parameters used throughout the query.
-- The lookback timestamp limits the amount of historical data processed.
WITH parameters AS (
    SELECT TIMESTAMP '2026-01-01 00:00:00' AS lookback
),

-- Aggregate journey exits by journey occurrence and exit reason.
-- This provides the number of members associated with each exit reason.
journey_occ_drop_reason AS (
	select journey_occurrence_id
        , reason_cd
        , reason_txt
		, count(identity_id) as member_nbr
	from udm.journey_exit
	where exit_dttm > (select lookback from parameters)
	group by journey_occurrence_id
        , reason_txt
        , reason_cd
),

-- Calculate the total number of exits for each journey occurrence.
-- This total is used to calculate the percentage contribution of each exit reason.
journey_occ_drop_total AS (
	select journey_occurrence_id
		, SUM(member_nbr) as exit_total
	from journey_occ_drop_reason
	group by journey_occurrence_id
),


-- Extract the journey end-node ID from the exit reason text when
-- the journey exit was caused by reaching an end node.
journey_drop_end_node_id as  (
	select *,

	    -- For "Reached End Node" exit reasons, extract the node ID
        -- so it can be joined to the journey node metadata.
		case when substr(reason_txt,1,16)='Reached End Node' then substr(reason_txt,18,36) 
			else ' ' end as end_node_id

	from journey_occ_drop_reason
)


-- Combine journey exit statistics with journey occurrence,
-- journey, and end-node metadata.
select j.journey_nm
		, o.journey_occurrence_num
		, d.reason_cd
		, n.node_nm as end_node_nm
		, d.reason_txt
		, d.member_nbr
		, dt.exit_total

		  -- Percentage of all exits for the journey occurrence represented by this exit reason.
          -- NULLIF prevents division by zero.
		, CAST(COALESCE(d.member_nbr, 0)/NULLIF(dt.exit_total, 0)*100 AS DECIMAL(6,2)) AS exit_pct

	from journey_drop_end_node_id d
	inner join journey_occ_drop_total dt on d.journey_occurrence_id = dt.journey_occurrence_id
	inner join udm.md_journey_occurrence o on d.journey_occurrence_id = o.journey_occurrence_id 
	inner join udm.md_journey j on  j.journey_id=o.journey_id

	-- Add the end-node name when an end-node ID could be derived from the exit reason.
	left join udm.MD_JOURNEY_NODE n on n.journey_node_id=d.end_node_id

	order by j.journey_nm
		, o.journey_occurrence_num
