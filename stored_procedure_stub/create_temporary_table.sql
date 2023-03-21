CREATE OR REPLACE PROCEDURE create_temporary_table(target_table_name VARCHAR, source_table_name VARCHAR)
  RETURNS VARIANT
  LANGUAGE JAVASCRIPT
  EXECUTE AS OWNER
AS 
$$
    var results = [];
    var sql_command = "CREATE OR REPLACE TEMPORARY TABLE " + TARGET_TABLE_NAME + " LIKE " + SOURCE_TABLE_NAME ;
    snowflake.execute({sqlText: sql_command});
    var success_message = "Table " + TARGET_TABLE_NAME.toUpperCase() + " successfully created.";
    results.push({"status": success_message}); 
    return results;
$$;