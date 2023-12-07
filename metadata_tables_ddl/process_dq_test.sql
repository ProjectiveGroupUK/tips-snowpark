CREATE OR REPLACE TABLE process_dq_test (
	process_dq_test_id                 NUMBER(38,0) NOT NULL AUTOINCREMENT,
	process_dq_test_name               VARCHAR(100) NOT NULL,
	process_dq_test_description        VARCHAR,
	process_dq_test_query_template     VARCHAR NOT NULL,
	process_dq_test_error_message      VARCHAR NOT NULL,
	active                             BOOLEAN NOT NULL DEFAULT TRUE,
	UNIQUE (process_dq_test_name),
	PRIMARY KEY (process_dq_test_id)
);

MERGE INTO tips_md_schema.process_dq_test a
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