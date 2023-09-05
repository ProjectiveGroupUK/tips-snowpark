CREATE OR REPLACE TABLE process_cmd_tgt_dq_test (
    process_cmd_tgt_dq_test_id  NUMBER(38,0) NOT NULL AUTOINCREMENT,
    tgt_name                    VARCHAR(100) NOT NULL,
    attribute_name              VARCHAR(100),
    process_dq_test_name        VARCHAR(100) NOT NULL,
    accepted_values             VARCHAR(16777216),
    query_binds                 VARCHAR,
    error_and_abort             BOOLEAN NOT NULL DEFAULT TRUE,
    active                      BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (process_cmd_tgt_dq_test_id)
);