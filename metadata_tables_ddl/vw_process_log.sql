create or replace view VW_PROCESS_LOG(
	PROCESS_LOG_ID,
	PROCESS_NAME,
	PROCESS_START_TIME,
	PROCESS_END_TIME,
	PROCESS_ELAPSED_TIME_IN_SECONDS,
	EXECUTE_FLAG,
	PROCESS_STATUS,
	PROCESS_ERROR,
	COMMAND_TYPE,
	SOURCE,
	TARGET,
	SQL,
	STEP_STATUS,
	EXECUTION_TIME_IN_SECS,
	COMMAND_STATUS,
	COMMAND_WARNING,
	COMMAND_ERROR
) as
select pl.process_log_id
, pl.process_name
, pl.process_start_time
, pl.process_end_time
, pl.process_elapsed_time_in_seconds
, pl.execute_flag
, pl.status AS process_status
, pl.error_message AS process_error
, stps.value:action::varchar AS command_type
, stps.value:parameters:source::varchar AS source
, stps.value:parameters:target::varchar AS target
, cmds.value:sql_cmd::varchar AS sql
, cmds.value:status::varchar AS step_status
, cmds.value:cmd_status:EXECUTION_TIME_IN_SECS AS execution_time_in_secs
, cmds.value:cmd_status:STATUS::varchar AS command_status
, cmds.value:warning_message::varchar AS command_warning
, cmds.value:error_message::varchar AS command_error
from process_log pl
, lateral flatten(input => parse_json(log_json:steps), outer => true) stps
, lateral flatten(input => parse_json(stps.value:commands), outer => true) cmds;