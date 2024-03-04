CREATE OR REPLACE TABLE process (
    process_id              NUMBER NOT NULL AUTOINCREMENT,
    process_name            VARCHAR(100) NOT NULL,
    process_description     VARCHAR,
    run_steps_in_parallel   BOOLEAN DEFAULT TRUE,
    active                  VARCHAR(1) DEFAULT 'Y'
);