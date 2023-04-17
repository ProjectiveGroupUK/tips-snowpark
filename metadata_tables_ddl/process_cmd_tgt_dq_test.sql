create or replace TABLE PROCESS_CMD_TGT_DQ_TEST (
	PROCESS_CMD_TGT_DQ_TEST_ID NUMBER(38,0) NOT NULL autoincrement,
	TGT_NAME VARCHAR(100) NOT NULL,
	ATTRIBUTE_NAME VARCHAR(100),
	PROCESS_DQ_TEST_NAME VARCHAR(100) NOT NULL,
	ACCEPTED_VALUES VARCHAR(16777216),
	ERROR_AND_ABORT BOOLEAN NOT NULL DEFAULT TRUE,
	ACTIVE BOOLEAN NOT NULL DEFAULT TRUE,
	primary key (PROCESS_CMD_TGT_DQ_TEST_ID)
);