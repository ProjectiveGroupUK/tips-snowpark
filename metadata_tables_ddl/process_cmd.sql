CREATE TABLE IF NOT EXISTS process_cmd (
    process_id                          NUMBER(38,0) NOT NULL,
    process_cmd_id                      NUMBER(38,0) NOT NULL,
    cmd_type                            VARCHAR(30) NOT NULL,
    cmd_src                             VARCHAR,
    cmd_tgt                             VARCHAR,
    cmd_where                           VARCHAR,
    cmd_binds                           VARCHAR,
    refresh_type                        VARCHAR(10),
    business_key                        VARCHAR(100),
    merge_on_fields                     VARCHAR,
    generate_merge_matched_clause       VARCHAR(1),
    generate_merge_non_matched_clause   VARCHAR(1),
    additional_fields                   VARCHAR,
    temp_table                          VARCHAR(1),
    cmd_pivot_by                        VARCHAR,
    cmd_pivot_field                     VARCHAR,
    dq_type                             VARCHAR(100),
    cmd_external_call                   VARCHAR,
    file_format_name                    VARCHAR,
    copy_into_file_paritition_by        VARCHAR,
    active                              VARCHAR(1) DEFAULT 'Y'
);

ALTER TABLE IF EXISTS process_cmd ADD COLUMN IF NOT EXISTS copy_auto_mapping VARCHAR(1);
ALTER TABLE IF EXISTS process_cmd ADD COLUMN IF NOT EXISTS copy_into_force VARCHAR(1);
ALTER TABLE IF EXISTS process_cmd ADD COLUMN IF NOT EXISTS parent_process_cmd_id VARCHAR NOT NULL DEFAULT 'NONE';
ALTER TABLE IF EXISTS process_cmd ADD COLUMN IF NOT EXISTS warehouse_size VARCHAR;