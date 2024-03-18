create or replace TABLE PROCESS_DQ_TEST (
	PROCESS_DQ_TEST_ID NUMBER(38,0) NOT NULL autoincrement,
	PROCESS_DQ_TEST_NAME VARCHAR(100) NOT NULL,
	PROCESS_DQ_TEST_DESCRIPTION VARCHAR(16777216),
	PROCESS_DQ_TEST_QUERY_TEMPLATE VARCHAR(16777216) NOT NULL,
	PROCESS_DQ_TEST_ERROR_MESSAGE VARCHAR(16777216) NOT NULL,
	ACTIVE BOOLEAN NOT NULL DEFAULT TRUE,
	unique (PROCESS_DQ_TEST_NAME),
	primary key (PROCESS_DQ_TEST_ID)
);

MERGE INTO services.process_dq_test a
USING (
  SELECT 'UNIQUE' process_dq_test_name
       , 'Check Uniqueness of a column in the table' process_dq_test_description
       , 'SELECT {COL_NAME}, COUNT(*) FROM {TAB_NAME} GROUP BY {COL_NAME} HAVING COUNT(*) > 1' process_dq_test_query_template
       , 'Non Unique Values found in {TAB_NAME}.{COL_NAME}' process_dq_test_error_message
  UNION ALL
  SELECT 'NOT_NULL' process_dq_test_name
       , 'Check that column value in the table are not null for any record' process_dq_test_description
       , 'SELECT {COL_NAME} FROM {TAB_NAME} WHERE {COL_NAME} IS NULL' process_dq_test_query_template
       , 'NULL Values found in {TAB_NAME}.{COL_NAME}' process_dq_test_error_message
  UNION ALL
  SELECT 'ACCEPTED_VALUES' process_dq_test_name
       , 'Check that column in the table contains only one of the accepted values' process_dq_test_description
       , 'SELECT {COL_NAME} FROM {TAB_NAME} WHERE {COL_NAME} NOT IN ({ACCEPTED_VALUES})' process_dq_test_query_template
       , 'Values other than {ACCEPTED_VALUES} found in {TAB_NAME}.{COL_NAME}' process_dq_test_error_message
  UNION ALL
  SELECT 'REFERENTIAL_INTEGRITY' process_dq_test_name
       , 'Check that values in a column of target table only contains values existing in referenced table' process_dq_test_description
       , 'SELECT {COL_NAME} FROM {TAB_NAME}  WHERE {COL_NAME} NOT IN (SELECT DISTINCT {:1} FROM {:2})' process_dq_test_query_template
       , 'Not all Values from {TAB_NAME}.{COL_NAME} exist in {:2}.{:1}' process_dq_test_error_message
) b
ON a.process_dq_test_name = b.process_dq_test_name
WHEN MATCHED THEN
UPDATE 
   SET process_dq_test_description = b.process_dq_test_description
     , process_dq_test_query_template = b.process_dq_test_query_template
     , process_dq_test_error_message = b.process_dq_test_error_message
WHEN NOT MATCHED THEN
INSERT (
    process_dq_test_name
  , process_dq_test_description
  , process_dq_test_query_template
  , process_dq_test_error_message
) 
VALUES (
    b.process_dq_test_name
  , b.process_dq_test_description
  , b.process_dq_test_query_template
  , b.process_dq_test_error_message
);