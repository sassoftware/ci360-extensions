-- Define parameters used throughout the query.
-- The lookback timestamp limits the amount of historical data processed.
WITH parameters AS (
    SELECT TIMESTAMP '2026-01-01 00:00:00' AS lookback
),


-- Aggregate suppressed audience members by journey occurrence, audience occurrence, and suppression reason.
journey_occ_id_suppress_reason  AS (
	select s.journey_occurrence_id
		, s.aud_occurrence_id
		, s.reason_cd
		, s.reason_txt
-- Count identity_id distinctly in case a segment based audience has the same id occur multiple segments 
		, count(distinct s.identity_id) as members_suppressed 
	from  udm.journey_suppression s 
	where suppression_dttm  > (select lookback from parameters)
	group by s.journey_occurrence_id
		, s.aud_occurrence_id
		, s.reason_cd
		, s.reason_txt
)


-- Enrich the aggregated suppression data with journey, journey occurrence,
-- audience occurrence, and audience metadata.
select j.journey_nm
	, jo.journey_occurrence_num
	, a.audience_nm
	, ao.audience_size_val
	, d.reason_cd
	, d.reason_txt
	, d.members_suppressed
from udm.md_journey j
inner join udm.md_journey_occurrence jo on j.journey_id=jo.journey_id
inner join journey_occ_id_suppress_reason d on d.journey_occurrence_id = jo.journey_occurrence_id 
inner join udm.md_audience_occurrence ao on ao.aud_occurrence_id=jo.aud_occurrence_id
inner join udm.md_audience a on a.audience_id=ao.audience_id;
