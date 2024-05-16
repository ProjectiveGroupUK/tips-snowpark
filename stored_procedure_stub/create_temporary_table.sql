CREATE OR REPLACE PROCEDURE create_temporary_table(target_table_name VARCHAR, source_table_name VARCHAR)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  EXECUTE AS OWNER
AS 
$$
    var sql_command = "CREATE OR REPLACE TEMPORARY TABLE " + TARGET_TABLE_NAME + " LIKE " + SOURCE_TABLE_NAME ;
    snowflake.execute({sqlText: sql_command});
    var success_message = "Table " + TARGET_TABLE_NAME.toUpperCase() + " successfully created.";
    return success_message;
$$;

GRANT OWNERSHIP ON PROCEDURE CREATE_TEMPORARY_TABLE(VARCHAR,VARCHAR) TO DATABASE ROLE DB_CREATE COPY CURRENT GRANTS;