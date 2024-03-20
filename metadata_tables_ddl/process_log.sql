CREATE TABLE IF NOT EXISTS process_log (
    process_log_id                      NUMBER(38,0) NOT NULL PRIMARY KEY DEFAULT PROCESS_LOG_SEQ.NEXTVAL,
    process_name                        VARCHAR(100) NOT NULL,
    process_log_created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    process_start_time                  TIMESTAMP,
    process_end_time                    TIMESTAMP,
    process_elapsed_time_in_seconds     INTEGER,
    execute_flag                        VARCHAR2(1) NOT NULL DEFAULT 'Y',
    status                              VARCHAR2(100) NOT NULL,
    error_message                       VARCHAR,
    log_json                            VARIANT NOT NULL
);

ALTER TABLE IF EXISTS process_log ADD COLUMN IF NOT EXISTS run_id VARCHAR;