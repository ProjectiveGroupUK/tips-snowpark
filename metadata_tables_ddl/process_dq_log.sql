CREATE TABLE IF NOT EXISTS process_dq_log (
    process_dq_log_id           NUMBER(38,0) NOT NULL AUTOINCREMENT,
    run_id                      VARCHAR NOT NULL,
    process_log_id              NUMBER(38,0) NOT NULL,
    tgt_name                    VARCHAR(100) NOT NULL,
    attribute_name              VARCHAR(100),
    dq_test_name                VARCHAR(100) NOT NULL,
    dq_test_query               VARCHAR,
    dq_test_result              VARIANT,
    start_time                  TIMESTAMP_NTZ(9),
    end_time                    TIMESTAMP_NTZ(9),
    elapsed_time_in_seconds     NUMBER(38,0),
    status                      VARCHAR(100) NOT NULL,
    status_message              VARCHAR
);