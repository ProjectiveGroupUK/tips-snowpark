create or replace TABLE PROCESS_DQ_LOG (
	PROCESS_DQ_LOG_ID NUMBER(38,0) NOT NULL autoincrement,
	PROCESS_LOG_ID NUMBER(38,0) NOT NULL,
	TGT_NAME VARCHAR(100) NOT NULL,
	ATTRIBUTE_NAME VARCHAR(100),
	DQ_TEST_NAME VARCHAR(100) NOT NULL,
	DQ_TEST_QUERY VARCHAR(16777216),
	DQ_TEST_RESULT VARIANT,
	START_TIME TIMESTAMP_NTZ(9),
	END_TIME TIMESTAMP_NTZ(9),
	ELAPSED_TIME_IN_SECONDS NUMBER(38,0),
	STATUS VARCHAR(100) NOT NULL,
	STATUS_MESSAGE VARCHAR(16777216)
);