create or replace view VW_PROCESS_LOG as
select pl.process_log_id
, pl.process_name
, pl.process_start_time
, pl.process_end_time
, pl.process_elapsed_time_in_seconds
, pl.execute_flag
, pl.status AS process_status
, pl.error_message AS process_error
, stps.value:process_cmd_id::int AS process_cmd_id
, cmds.value:cmd_sequence::int AS cmd_sequence
, stps.value:action::varchar AS command_type
, stps.value:parameters:source::varchar AS source
, stps.value:parameters:target::varchar AS target
, cmds.value:sql_cmd::varchar AS sql
, cmds.value:status::varchar AS step_status
, cmds.value:cmd_status:ROWS_INSERTED AS ROWS_INSERTED
, cmds.value:cmd_status:ROWS_UPDATED AS ROWS_UPDATED
, cmds.value:cmd_status:ROWS_DELETED AS ROWS_DELETED
, cmds.value:cmd_status:ROWS_LOADED AS ROWS_LOADED
, cmds.value:cmd_status:ROWS_UNLOADED AS ROWS_UNLOADED
, cmds.value:cmd_status:EXECUTION_TIME_IN_SECS AS execution_time_in_secs
, cmds.value:cmd_status:STATUS::varchar AS command_status
, cmds.value:warning_message::varchar AS command_warning
, cmds.value:error_message::varchar AS command_error
from process_log pl
, lateral flatten(input => parse_json(log_json:steps), outer => true) stps
, lateral flatten(input => parse_json(stps.value:commands), outer => true) cmds
;