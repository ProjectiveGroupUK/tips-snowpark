create or replace TABLE PROCESS_CMD (
    PROCESS_ID                          NUMBER(38,0) NOT NULL,
    PROCESS_CMD_ID                      NUMBER(38,0) NOT NULL,
    CMD_TYPE                            VARCHAR(30) NOT NULL,
    CMD_SRC                             VARCHAR,
    CMD_TGT                             VARCHAR,
    CMD_WHERE                           VARCHAR,
    CMD_BINDS                           VARCHAR,
    REFRESH_TYPE                        VARCHAR(10),
    BUSINESS_KEY                        VARCHAR(100),
    MERGE_ON_FIELDS                     VARCHAR,
    GENERATE_MERGE_MATCHED_CLAUSE       VARCHAR(1),
    GENERATE_MERGE_NON_MATCHED_CLAUSE   VARCHAR(1),
    ADDITIONAL_FIELDS                   VARCHAR,
    COPY_AUTO_MAPPING                   VARCHAR(1),
    COPY_INTO_FORCE                     VARCHAR(1),
    TEMP_TABLE                          VARCHAR(1),
    CMD_PIVOT_BY                        VARCHAR,
    CMD_PIVOT_FIELD                     VARCHAR,
    DQ_TYPE                             VARCHAR(100),
    CMD_EXTERNAL_CALL                   VARCHAR,
    FILE_FORMAT_NAME                    VARCHAR,
    COPY_INTO_FILE_PARITITION_BY        VARCHAR,
    ACTIVE                              VARCHAR(1) DEFAULT 'Y'
);

ALTER TABLE IF EXISTS process_cmd ADD COLUMN IF NOT EXISTS copy_auto_mapping VARCHAR(1);
ALTER TABLE IF EXISTS process_cmd ADD COLUMN IF NOT EXISTS copy_into_force VARCHAR(1);
ALTER TABLE IF EXISTS process_cmd ADD COLUMN IF NOT EXISTS parent_process_cmd_id VARCHAR NOT NULL DEFAULT 'NONE';
ALTER TABLE IF EXISTS process_cmd ADD COLUMN IF NOT EXISTS warehouse_size VARCHAR;