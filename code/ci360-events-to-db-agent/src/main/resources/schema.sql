CREATE TABLE IF NOT EXISTS contacthistory (
datahub_id varchar(36),
subject_id varchar(18),
contact_id varchar(36),
contact_dttm_utc timestamp(6) without time zone,
task_id varchar(36),
channel_type varchar(36),
insert_ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);