create or replace TABLE PROCESS_CMD (
    PROCESS_ID                          NUMBER(38,0) NOT NULL,
    PROCESS_CMD_ID                      NUMBER(38,0) NOT NULL,
    CMD_TYPE                            VARCHAR(20) NOT NULL,
    CMD_SRC                             VARCHAR,
    CMD_TGT                             VARCHAR NOT NULL,
    CMD_WHERE                           VARCHAR,
    CMD_BINDS                           VARCHAR,
    REFRESH_TYPE                        VARCHAR(10),
    BUSINESS_KEY                        VARCHAR(100),
    MERGE_ON_FIELDS                     VARCHAR,
    GENERATE_MERGE_MATCHED_CLAUSE       VARCHAR(1),
    GENERATE_MERGE_NON_MATCHED_CLAUSE   VARCHAR(1),
    ADDITIONAL_FIELDS                   VARCHAR,
    TEMP_TABLE                          VARCHAR(1),
    CMD_PIVOT_BY                        VARCHAR,
    CMD_PIVOT_FIELD                     VARCHAR,
    DQ_TYPE                             VARCHAR(100),
    CMD_EXTERNAL_CALL                   VARCHAR,
    FILE_FORMAT_NAME                    VARCHAR,
    COPY_INTO_FILE_PARITITION_BY        VARCHAR,
    ACTIVE                              VARCHAR(1) DEFAULT 'Y'
);