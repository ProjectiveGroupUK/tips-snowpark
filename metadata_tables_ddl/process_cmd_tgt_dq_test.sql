CREATE TABLE IF NOT EXISTS process_cmd_tgt_dq_test (
    process_cmd_tgt_dq_test_id  NUMBER(38,0) NOT NULL AUTOINCREMENT,
    tgt_name                    VARCHAR(100) NOT NULL,
    attribute_name              VARCHAR(100),
    process_dq_test_name        VARCHAR(100) NOT NULL,
    accepted_values             VARCHAR,
    error_and_abort             BOOLEAN NOT NULL DEFAULT TRUE,
    active                      BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (process_cmd_tgt_dq_test_id)
);

ALTER TABLE IF EXISTS process_cmd_tgt_dq_test ADD COLUMN IF NOT EXISTS query_binds VARCHAR;
