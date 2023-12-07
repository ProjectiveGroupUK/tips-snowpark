CREATE OR REPLACE TABLE process (
    process_id          NUMBER NOT NULL AUTOINCREMENT,
    process_name        VARCHAR(100) NOT NULL,
    process_description VARCHAR,
    active              VARCHAR(1) DEFAULT 'Y'
);