CREATE TABLE IF NOT EXISTS process (
    process_id              NUMBER NOT NULL AUTOINCREMENT,
    process_name            VARCHAR(100) NOT NULL,
    process_description     VARCHAR,
    active                  VARCHAR(1) DEFAULT 'Y'
);

ALTER TABLE IF EXISTS process ADD COLUMN IF NOT EXISTS run_steps_in_parallel BOOLEAN DEFAULT FALSE;