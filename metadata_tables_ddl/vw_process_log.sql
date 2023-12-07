CREATE OR REPLACE VIEW vw_process_log AS
SELECT pl.process_log_id
     , pl.process_name
     , pl.process_start_time
     , pl.process_end_time
     , pl.process_elapsed_time_in_seconds
     , pl.execute_flag
     , pl.status                                    AS process_status
     , pl.error_message                             AS process_error
     , stps.value:process_cmd_id::INT               AS process_cmd_id
     , cmds.value:cmd_sequence::INT                 AS cmd_sequence
     , stps.value:action::VARCHAR                   AS command_type
     , stps.value:parameters:source::VARCHAR        AS source
     , stps.value:parameters:target::VARCHAR        AS target
     , cmds.value:sql_cmd::VARCHAR                  AS sql
     , cmds.value:status::VARCHAR                   AS step_status
     /* Last element in following have to be in uppercase, as that's how it is captured in JSON -> */
     , cmds.value:cmd_status:ROWS_INSERTED          AS rows_inserted
     , cmds.value:cmd_status:ROWS_UPDATED           AS rows_updated
     , cmds.value:cmd_status:ROWS_DELETED           AS rows_deleted
     , cmds.value:cmd_status:ROWS_LOADED            AS rows_loaded
     , cmds.value:cmd_status:ROWS_UNLOADED          AS rows_unloaded
     , cmds.value:cmd_status:EXECUTION_TIME_IN_SECS AS execution_time_in_secs
     , cmds.value:cmd_status:STATUS::VARCHAR        AS command_status
     /* <- for the above lines */
     , cmds.value:warning_message::VARCHAR          AS command_warning
     , cmds.value:error_message::VARCHAR            AS command_error
  FROM process_log pl
, LATERAL FLATTEN(input => PARSE_JSON(log_json:steps), outer => TRUE) stps
, LATERAL FLATTEN(input => PARSE_JSON(stps.value:commands), outer => TRUE) cmds
;