# Reference Guide
## Metadata Tables
### PROCESS
This is the table where you define information about data pipeline e.g., Name of Pipeline and other information.

| Column Name | Description                     |
|-------------| ------------------------------- |
| PROCESS_ID | Sequentially generated ID.<p> This is defined as Auto increment on table, so doesn't need to be included in DMLs</p> |
| PROCESS_NAME | Enter Name of Data Pipeline.<p> This value is passed as parameters when TiPS is executed. <br>**No whitespaces to be used in process name and preferably use Uppercase**</p> |
| PROCESS_DESCRIPTION | Description about Data Pipeline.<p> This is optional field, but good to have proper description about the pipeline for others to easily understand</p> |
| ACTIVE | Active (Y) / Inactive (N) flag.<p> When set to N (inactive), data pipeline can be disabled and TiPS will not execute it</p> |

### PROCESS_CMD
This is the table where you populate information about each step in a data pipeline. There are several columns in this table, some of which are specific to command types. Out of those some are mandatory, and some are optional. Further details about command types are documented [below](#command-types) 

| Column Name | Description                     |
|-------------| ------------------------------- |
| PROCESS_ID | Foreign Key to PROCESS_ID in PROCESS table<p>This needs to match ID of data pipeline defined in PROCESS table</p>|
| PROCESS_CMD_ID | Manually assigned sequential ID for Data Pipeline Step<p>When TiPS is executed, steps of a data pipeline run serially in sequential order. Order of the step is identified using this ID. It is advisable to leave gaps between the IDs when setting up the data pipeline initially, so that if there is a need to add an interim step later, it can be inserted in between without needing to reassign all IDs. E.g., 10,20,30,40... or 100,200,300,400... can be assigned initially which leave sufficient gap for steps to be inserted in between, if need be, later. Also in a big team, where multiple data engineers might be working on same data pipeline in same development iteration (sprint), mechanism needs to be in place so that data engineers don't end up using overlapping IDs</p> |
| CMD_TYPE | This describes the operation type. Further details are given [below](#command-types) <p>E.g., APPEND / REFRESH / DELETE / TRUNCATE etc. <br>**All CAPS without spaces**</br></p>  |
| CMD_SRC | Specify the name of source of data here. <p>This is usually a data table or a view that encapsulates the transformation business logic. <br>**Please include schema name with the object name e.g. [SCHEMA NAME].[OBJECT NAME] and all in CAPS please**</p> |
| CMD_TGT | Specify the name of target destination of data here. <p>This is usually a table. <br>**Please include schema name with the object name e.g. [SCHEMA NAME].[OBJECT NAME] and all in CAPS please**</p> |
| CMD_WHERE | This is where you can specify a filter clause that gets added to source as a WHERE clause at run time. <p>**WHERE** keyword should not be included and numbered bind variables can be used for which actual bind replacements are mentioned in CMD_BINDS <br>**E.g.</br><p>"C_MKTSEGMENT = :2 AND COBID = :1"**</p>In the above example values for bind variables are passed at run time, and derivation of value for bind variable according to the sequence is derived from CMD_BINDS</p> |
| CMD_BINDS | If bind variables are used in the step, here you specify the name for bind variables, delimited by Pipe **"\|"** symbol. <p>Bind variable values are passed in at runtime in a JSON format. Names of bind variables defined here are interpreted as keys from the variable values JSON object passed in at run-time</p> |
| REFRESH_TYPE | This is only applicable for REFRESH command type. Acceptable values are **DI/TI/OI** <p>**DI (Delete Insert)** - Before inserting the data in target, a delete command is run (where optionally filter clause can be added through CMD_WHERE).</p><p>**TI (Truncate Insert)** - Before inserting the data in target, truncate command on target is run.</p><p>**OI (Overwrite Insert)** - Before inserting the data in target, any existing data is removed from target. This works similar to truncate, with the caveat that TRUNCATE is a DDL command invoking a commit to the transaction where OVERWRITE doesn't commits the transaction immediately after delete, thus tables is rolled back to previous state if INSERT DML throws an error.</p> |
| BUSINESS_KEY | This is only applicable for PUBLISH_SCD2_DIM command type. It is column(s) delimited by Pipe **"\|"** symbol that defines a business key (also referred as natural key) for a dimension table. <p>For a slowly changing dimension, this is combination of key columns that uniquely identifies a row in the dimension (not a surrogate key), excluding record effective dates and/or current record flag</p> |
| MERGE_ON_FIELDS | This is only applicable for MERGE command type. Here you specify columns that are to be used in generated MERGE SQL in the **ON** join clause. Multiple fields delimited by Pipe **"\|"** symbol |
| GENERATE_MERGE_MATCHED_CLAUSE | This is only applicable for MERGE command type. Here you specify whether ON MATCHED CLAUSE is to be generated in generated MERGE DML. Accepted values are Y/N. When Y is selected, ON MATCHED CLAUSE is generated which runs an UPDATE operation |
| GENERATE_MERGE_NON_MATCHED_CLAUSE | This is only applicable for MERGE command type. Here you specify whether ON NOT MATCHED CLAUSE is to be generated in generated MERGE DML. Accepted values are Y/N. When Y is selected, ON NOT MATCHED CLAUSE is generated which runs an INSERT operation|
| ADDITIONAL_FIELDS | This is where you specify any additional columns/fields to be added to generated SELECT clause from source, which is not available in source. <p>E.g.</p><p>TO_NUMBER(:1) COBID</p><p>would add a column to generated SELECT statement, where it is transforming bind variable value passed in at run time, and aliased as COBID, which would then become part of INSERT/MERGE statement</p> |
| TEMP_TABLE | Acceptable values - Y/N/NULL <p>When set to Y, this would trigger creating a temporary table with the same name as target in the same schema as target before the operation of the step is run.</p><p>This feature utilises special functionality that has been introduced in Snowflake, that you can have permanent (or transient) table and a temporary table with the same name and temporary table is then given priority in the current running session.</p><p>In TiPS we utilise this functionality where a data pipeline can be run concurrently withing multiple sessions with its own bind variables and dataset are consistently transformed and published at session level |
| CMD_PIVOT_BY | This is only applicable for COPY_INTO_FILE command type. If you are looking to apply a PIVOT in the SQL query in the COPY INTO statement, then this can be used. This field will dictate which column's values are to be pivoted e.g., REPORTING_CCY |
| CMD_PIVOT_FIELD | This is only applicable for COPY_INTO_FILE command type. If you are looking to apply a PIVOT in the SQL query in the COPY INTO statement, then this can be used. This field will be the aggregation of the field for the values which are included in the pivot e.g., SUM(REPORTING_CURRENCY) |
| ACTIVE | Active (Y) / Inactive (N) flag.<p> When set to N (inactive), data pipeline step can be disabled and TiPS will skip that step while execute the pipeline |
| FILE_FORMAT_NAME | This option is applicable to COPY_INTO_FILE and COPY_INTO_TABLE command types. <p>If a file format has been defined in the database, that can be used. <br>**Please include schema name with file format name e.g. [SCHEMA NAME].[FILE FORMAT NAME] and all in CAPS please**</p> |
| COPY_INTO_FILE_PARITITION_BY | This option is applicable to COPY_INTO_FILE command type. This adds PARTITION BY clause in generated COPY INTO FILE command. COPY_INTO_FILE_PARITITION_BY field needs to be an SQL expression that outputs a string. The dataset specified by CMD_SRC will then be split into individual files based on the output of the expression. A directory will be created in the stage specified by CMD_TGT which will be named the same as the partition clause. The data will then be output into this location in the stage. |

### PROCESS_LOG
This table holds logging information about each run of TiPS. This table is populated automatically at the end of execution of TiPS

| Column Name | Description                     |
|-------------| ------------------------------- |
| PROCESS_LOG_ID | Sequentially generated ID|
| PROCESS_NAME | Name of Data Pipeline that has been executed. |
| PROCESS_LOG_CREATED_AT | Timestamp at which log records have been inserted to the table |
| PROCESS_START_TIME | Timestamp of start of execution of Data Pipeline |
| PROCESS_END_TIME | Timestamp of completion of execution of Data Pipeline |
| PROCESS_ELAPSED_TIME_IN_SECONDS | Total time (in seconds) taken in execution of Data Pipeline |
| EXECUTE_FLAG | Whether Data Pipeline was invoked with Execution Status. When EXECUTE_FLAG is passed as N, SQL statements are only generated and logged but not executed in the database |
| STATUS | Status of Data Pipeline Execution |
| ERROR_MESSAGE | If any steps errored, top level error message is populated here |
| LOG_JSON | Complete Log information of Data Pipeline in JSON format. View `VW_PROCESS_LOG` displays flattened information of this column |

### PROCESS_DQ_TEST
This table is populated with data relating to Data Quality Tests. This table is shipped with some standard DQ Test definitions.

| Column Name | Description                     |
|-------------| ------------------------------- |
| PROCESS_DQ_TEST_ID | Sequentially generated ID|
| PROCESS_DQ_TEST_NAME | Uniquely identifiable Name for Data Quality Test |
| PROCESS_DQ_TEST_DESCRIPTION | Descriptive information about Data Quality Test |
| PROCESS_DQ_TEST_QUERY_TEMPLATE | Query template to be used when running Data Quality Test. Identifiers within curly braces `{}` are replaces with actual values at run time |
| PROCESS_DQ_TEST_ERROR_MESSAGE | Error Message to display when Test fails |
| ACTIVE | TRUE/FALSE<p> When FALSE, data quality test would not run |


### PROCESS_CMD_TGT_DQ_TEST
This is the table that you populate with the information to enforce a predefined Data Quality test to a target (table) and additionally an attribute (column) 

| Column Name | Description                     |
|-------------| ------------------------------- |
| PROCESS_CMD_TGT_DQ_TEST_ID | Sequentially generated ID|
| TGT_NAME | Specify the name of target on which Data Quality Test is to be run. <p>This is usually a table. <br>**Please include schema name with the object name e.g. [SCHEMA NAME].[OBJECT NAME] and all in CAPS please.**</p><p>This should match target name defined on `PROCESS_CMD` table |
| ATTRIBUTE_NAME | Enter column name on which Data Quality Test is to be run |
| ACCEPTED_VALUES | For "Accepted Values" test, this should contain comma separated values that are acceptable in the target.<p>E.g.</p><p>`'AFRICA','MIDDLE EAST','EUROPE','AMERICA'`</p> |
| ERROR_AND_ABORT | TRUE/FALSE, indicating whether the process (data pipeline) should produce error and abort execution when this data quality test fails. When FALSE, process would just log warning and process would continue |
| ACTIVE | TRUE/FALSE<p> When FALSE, data quality test would not run |

### PROCESS_DQ_LOG
This table holds logging information about each Data Quality test expected withing a data pipeline when TiPS is run. This log is also associated to data in `PROCESS_LOG` table.

| Column Name | Description                     |
|-------------| ------------------------------- |
| PROCESS_DQ_LOG_ID | Sequentially generated ID |
| PROCESS_LOG_ID | ID linking to record in `PROCESS_LOG` table |
| TGT_NAME | Name of target on which data quality test was executed |
| ATTRIBUTE_NAME | Attribute/Column on which data quality test was executed |
| DQ_TEST_NAME | Data Quality Test Name |
| DQ_TEST_QUERY | Transposed Query executed for the test |
| DQ_TEST_RESULT | Array of values causing failure. For successful test, it should be an empty array `[]` |
| START_TIME | Timestamp of start of execution of DQ Test Query |
| END_TIME | Timestamp of completion of execution of DQ Test Query |
| ELAPSED_TIME_IN_SECONDS | Total time (in seconds) taken in execution of DQ Test Query |
| STATUS | Status [PASSED or ERROR or WARNING] of DQ Test |
| STATUS_MESSAGE | Warning or Error Message returned |

## Command Types
### APPEND
This effectively generates an "INSERT INTO [target table] ([columns]) SELECT [columns] FROM [source] additionally WHERE", if applicable

Following are the fields applicable for APPEND command type:

| Field Name | Mandatory? | Description |
| ---------- | :-------: |-------------|
| CMD_SRC | Yes | Specify the name of source of data here. <p>This is usually a data table or a view that encapsulates the transformation business logic. <br>**Please include schema name with the object name e.g. [SCHEMA NAME].[OBJECT NAME] and all in CAPS please**</p> |
| CMD_TGT | Yes | Specify the name of target destination of data here. <p>This is usually a table. <br>**Please include schema name with the object name e.g. [SCHEMA NAME].[OBJECT NAME] and all in CAPS please**</p> |
| CMD_WHERE | No | This is where you can specify a filter clause that gets added to source as a WHERE clause at run time. <p>**WHERE** keyword should not be included and numbered bind variables can be used for which actual bind replacements are mentioned in CMD_BINDS <br>**E.g.</br><p>"C_MKTSEGMENT = :2 AND COBID = :1"**</p>In the above example values for bind variables are passed at run time, and derivation of value for bind variable according to the sequence is derived from CMD_BINDS</p> |
| CMD_BINDS | No | If bind variables are used in the step, here you specify the name for bind variables, delimited by Pipe **"\|"** symbol. <p>Bind variable values are passed in at runtime in a JSON format. Names of bind variables defined here are interpreted as keys from the variable values JSON object passed in at run-time</p> |
| ADDITIONAL_FIELDS | No | This is where you specify any additional columns/fields to be added to generated SELECT clause from source, which is not available in source. <p>E.g.</p><p>TO_NUMBER(:1) COBID</p><p>would add a column to generated SELECT statement, where it is transforming bind variable value passed in at run time, and aliased as COBID, which would then become part of INSERT/MERGE statement</p> |
| TEMP_TABLE | No | Acceptable values - Y/N/NULL <p>When set to Y, this would trigger creating a temporary table with the same name as target in the same schema as target before the operation of the step is run.</p><p>This feature utilises special functionality that has been introduced in Snowflake, that you can have permanent (or transient) table and a temporary table with the same name and temporary table is then given priority in the current running session.</p><p>In TiPS we utilise this functionality where a data pipeline can be run concurrently withing multiple sessions with its own bind variables and dataset are consistently transformed and published at session level</p> |

### COPY_INTO_FILE
This command type is for outputting data from a table/view into a file into an internal user stage or an internal named stage or an external stage.

Following are the fields applicable for COPY_INTO_FILE command type:

| Field Name | Mandatory? | Description |
| ---------- | :-------: |-------------|
| CMD_SRC | Yes | Specify the name of source of data here. <p>This is usually a data table or a view that would provide data in desired form. <br>**Please include schema name with the object name e.g. [SCHEMA NAME].[OBJECT NAME] and all in CAPS please**</p> |
| CMD_TGT | Yes | This is the file location for outputting data from source to this file. If the PARTITION_CLAUSE is not being used, then the CMD_TGT should be the exact path and filename that should be created. <p>If the PARTTION_CLAUSE is being used, then this field should only contain a stage and a path.</p><p>For example, if the file should go into a user stage in the XYZ directory it should be @~/XYZ.</p><p>In both cases, it is permissible to use any BIND variables as part of the name i.e., @~/:1/XYZ would create a directory based on the first bind variable followed by XYZ.</p> |
| CMD_WHERE | No | This is where you can specify a filter clause that gets added to source as a WHERE clause at run time. <p>**WHERE** keyword should not be included and numbered bind variables can be used for which actual bind replacements are mentioned in CMD_BINDS <br>**E.g.</br><p>"C_MKTSEGMENT = :2 AND COBID = :1"**</p>In the above example values for bind variables are passed at run time, and derivation of value for bind variable according to the sequence is derived from CMD_BINDS</p> |
| CMD_BINDS | No | If bind variables are used in the step, here you specify the name for bind variables, delimited by Pipe **"\|"** symbol. <p>Bind variable values are passed in at runtime in a JSON format. Names of bind variables defined here are interpreted as keys from the variable values JSON object passed in at run-time</p> |
| CMD_PIVOT_BY | No | If you are looking to apply a PIVOT in the SQL query in the COPY INTO statement, then this can be used. This field will dictate which column's values are to be pivoted e.g., REPORTING_CCY |
| CMD_PIVOT_FIELD | No | If you are looking to apply a PIVOT in the SQL query in the COPY INTO statement, then this can be used. This field will be the aggregation of the field for the values which are included in the pivot e.g., SUM(REPORTING_CURRENCY) |
| FILE_FORMAT_NAME | No | If a file format has been defined in the database with all applicable configurations, that this field can be used. <br>**Please include schema name with file format name e.g. [SCHEMA NAME].[FILE FORMAT NAME] and all in CAPS please**</br><p>If this field is omitted, then File Type "CSV" and compression "GZIP" is used by default |
| COPY_INTO_FILE_PARITITION_BY | No | This field is to be populated when you want to apply a PARTITION BY clause in generated COPY INTO FILE command. COPY_INTO_FILE_PARITITION_BY field needs to be an SQL expression that outputs a string. The dataset specified by CMD_SRC will then be split into individual files based on the output of the expression. A directory will be created in the stage specified by CMD_TGT which will be named the same as the partition clause.  The data will then be output into this location in the stage. |

### COPY_INTO_TABLE
This command type is for loading data from a staged file to the database table. Stage file can be stored in an internal user stage or an internal named stage or an external stage.

Following are the fields applicable for COPY_INTO_TABLE command type:

| Field Name | Mandatory? | Description |
| ---------- | :-------: |-------------|
| CMD_SRC | Yes | This is the file location with data to be loaded. CMD_SRC should be the exact path and filename that should be loaded. <p>It is permissible to use any BIND variables as part of the name i.e., @~/:1/XYZ/ABC.csv would create a directory based on the first bind variable followed by XYZ.</p><p>E.g.,</p><p>@tips/EXTRACTS/:1/PUBLISH_CUSTOMER/CUSTOMER.csv</p><p>When using File Format, extension can be omitted</p> |
| CMD_TGT | Yes | This is the name of table into which data from file is to be loaded<p>**Please include schema name along with table name, and all in CAPS**</p> |
| CMD_BINDS | No | If bind variables are used in the step, here you specify the name for bind variables, delimited by Pipe **"\|"** symbol. <p>Bind variable values are passed in at runtime in a JSON format. Names of bind variables defined here are interpreted as keys from the variable values JSON object passed in at run-time</p> |
| FILE_FORMAT_NAME | No | If a file format has been defined in the database with all applicable configurations, that this field can be used. <br>**Please include schema name with file format name e.g. [SCHEMA NAME].[FILE FORMAT NAME] and all in CAPS please**</br><p>If this field is omitted, then File Type "CSV" and compression "GZIP" is used by default |

### DELETE
This effectively generates a "DELETE FROM [target table] additionally WHERE", if applicable

Following are the fields applicable for DELETE command type:

| Field Name | Mandatory? | Description |
| ---------- | :-------: |-------------|
| CMD_TGT | Yes | Specify the name of table from which data is to be deleted. <p>**Please include schema name with the object name e.g. [SCHEMA NAME].[OBJECT NAME] and all in CAPS please**</p> |
| CMD_WHERE | No | This is where you can specify a filter clause that gets added as a WHERE clause at run time. <p>**WHERE** keyword should not be included and numbered bind variables can be used for which actual bind replacements are mentioned in CMD_BINDS <br>**E.g.</br><p>"C_MKTSEGMENT = :2 AND COBID = :1"**</p>In the above example values for bind variables are passed at run time, and derivation of value for bind variable according to the sequence is derived from CMD_BINDS</p> |
| CMD_BINDS | No | If bind variables are used in the step, here you specify the name for bind variables, delimited by Pipe **"\|"** symbol. <p>Bind variable values are passed in at runtime in a JSON format. Names of bind variables defined here are interpreted as keys from the variable values JSON object passed in at run-time</p> |

### DQ_TEST
This command type is for specifying Data Quality Tests within data pipeline. All active Data Quality Tests defined on the target that is specified here, are run within this step.

Following are the fields applicable for DQ_TEST command type:

| Field Name | Mandatory? | Description |
| ---------- | :-------: |-------------|
| CMD_TGT | Yes | Specify the name of table/view on which data quality tests are to be run from which data is to be deleted.<br><p>*This can also accept pipe delimited multiple targets, which gives a flexibility to define running of DQ Tests on multiple targets in a single step.*</p><p>**Please include schema name with the object name e.g. [SCHEMA NAME].[OBJECT NAME] and all in CAPS please**</p> |
| CMD_WHERE | No | This is where you can specify a filter clause that gets added as a WHERE clause at run time. <p>**WHERE** keyword should not be included and numbered bind variables can be used for which actual bind replacements are mentioned in CMD_BINDS <br>**E.g.</br><p>"C_MKTSEGMENT = :2 AND COBID = :1"**</p>In the above example values for bind variables are passed at run time, and derivation of value for bind variable according to the sequence is derived from CMD_BINDS</p> |
| CMD_BINDS | No | If bind variables are used in the step, here you specify the name for bind variables, delimited by Pipe **"\|"** symbol. <p>Bind variable values are passed in at runtime in a JSON format. Names of bind variables defined here are interpreted as keys from the variable values JSON object passed in at run-time</p> |

### MERGE
This command type is where we want to use a MERGE statement. It supports either WHEN MATCHED or WHEN NOT MATCHED or both.

Following are the fields applicable for MERGE command type:

| Field Name | Mandatory? | Description |
| ---------- | :-------: |-------------|
| CMD_SRC | Yes | Specify the name of source of data here. <p>This is usually a data table or a view that encapculates the transformation business logic. <br>**Please include schema name with the object name e.g. [SCHEMA NAME].[OBJECT NAME] and all in CAPS please**</p> |
| CMD_TGT | Yes | Specify the name of target destination of data here. <p>This is usually a table. <br>**Please include schema name with the object name e.g. [SCHEMA NAME].[OBJECT NAME] and all in CAPS please**</p> |
| CMD_WHERE | No | This is where you can specify a filter clause that gets added to source as a WHERE clause at run time. <p>**WHERE** keyword should not be included and numbered bind variables can be used for which actual bind replacements are mentioned in CMD_BINDS <br>**E.g.</br><p>"C_MKTSEGMENT = :2 AND COBID = :1"**</p>In the above example values for bind variables are passed at run time, and derivation of value for bind variable according to the sequence is derived from CMD_BINDS</p> |
| CMD_BINDS | No | If bind variables are used in the step, here you specify the name for bind variables, delimited by Pipe **"\|"** symbol. <p>Bind variable values are passed in at runtime in a JSON format. Names of bind variables defined here are interpreted as keys from the variable values JSON object passed in at run-time</p> |
| GENERATE_MERGE_MATCHED_CLAUSE | No | Acceptable values are Y/N. When set to Y, "WHEN MATCHED UPDATE" clause is added to generated MERGE statement<br><p>***Either this field or GENERATE_MERGE_NON_MATCHED_CLAUSE should be set to Y*** |
| GENERATE_MERGE_NON_MATCHED_CLAUSE | No | Acceptable values are Y/N. When set to Y, "WHEN NOT MATCHED INSERT" clause is added to generated MERGE statement<br><p>***Either this field or GENERATE_MERGE_MATCHED_CLAUSE should be set to Y*** |
| ADDITIONAL_FIELDS | No | This is where you specify any additional columns/fields to be added to generated SELECT clause from source, which is not available in source. <p>E.g.</p><p>TO_NUMBER(:1) COBID</p><p>would add a column to generated SELECT statement, where it is transforming bind variable value passed in at run time, and aliased as COBID, which would then become part of INSERT/MERGE statement</p> |
| TEMP_TABLE | No | Acceptable values - Y/N/NULL <p>When set to Y, this would trigger creating a temporary table with the same name as target in the same schema as target before the operation of the step is run.</p><p>This feature utilises special functionality that has been introduced in Snowflake, that you can have permanent (or transient) table and a temporary table with the same name and temporary table is then given priority in the current running session.</p><p>In TiPS we utilise this functionality where a data pipeline can be run concurrently withing multiple sessions with its own bind variables and dataset are consistently transformed and published at session level</p> |

### PUBLISH_SCD2_DIM
This command type is specifically created for populating data to SCD (Slowly Changing Dimension) Type 2, where updates to attributes of dimension are handled by creating a version of record with latest values and closing off previous version. This is done by setting appropriate values to "EFFECTIVE_START_DATE", "EFFECTIVE_END_DATE" and "IS_CURRENT_ROW" columns.

Following are the fields applicable for PUBLISH_SCD2_DIM command type:

| Field Name | Mandatory? | Description |
| ---------- | :-------: |-------------|
| CMD_SRC | Yes | Specify the name of source of data here. <p>This is usually a data table or a view that encapsulates the transformation business logic. <br>**Please include schema name with the object name e.g. [SCHEMA NAME].[OBJECT NAME] and all in CAPS please**</p> |
| CMD_TGT | Yes | Specify the name of target destination of data here. <p>This is usually a table. <br>**Please include schema name with the object name e.g. [SCHEMA NAME].[OBJECT NAME] and all in CAPS please**</p> |
| BUSINESS_KEY | Yes | It is column(s) delimitted by Pipe **"\|"** symbol that define a business key (also referred as natural key) for a dimension table. <p>For a slowly changing dimension, this is combination of key columns that uniquely identifies a row in the dimension (not a surrogate key), excluding record effective dates and/or current record flag |
| CMD_WHERE | No | This is where you can specify a filter clause that gets added to source as a WHERE clause at run time. <p>**WHERE** keyword should not be included and numbered bind variables can be used for which actual bind replacements are mentioned in CMD_BINDS <br>**E.g.</br><p>"C_MKTSEGMENT = :2 AND COBID = :1"**</p>In the above example values for bind variables are passed at run time, and derivation of value for bind variable according to the sequence is derived from CMD_BINDS</p> |
| CMD_BINDS | No | If bind variables are used in the step, here you specify the name for bind variables, delimited by Pipe **"\|"** symbol. <p>Bind variable values are passed in at runtime in a JSON format. Names of bind variables defined here are interpretted as keys from the variable values JSON object passed in at run-time</p> |
| ADDITIONAL_FIELDS | No | This is where you specify any additional columns/fields to be added to generated SELECT clause from source, which is not available in source. <p>E.g.</p><p>TO_NUMBER(:1) COBID</p><p>would add a column to generated SELECT statement, where it is transforming bind variable value passed in at run time, and aliased as COBID, which would then become part of INSERT/MERGE statement</p> |
| TEMP_TABLE | No | Acceptable values - Y/N/NULL <p>When set to Y, this would trigger creating a temporary table with the same name as target in the same schema as target, before the operation of the step is run.</p><p>This feature utilises special functionality that has been introduced in Snowflake, that you can have permanent(or transient) table and a temporary table with the same name and temporary table is then given priority in the current running session.</p><p>In TiPS we utilise this functionality where a data pipeline can be run concurrently withing multiple sessions with its own bind variables and dataset are consistently transformed and published at session level</p> |

### REFRESH
This command type is for running a DELETE/TRUNCATE SQL command on target and then consecutively running INSERT SQL command. REFRESH command type supports "DELETE then INSERT", "OVERWRITE INSERT" or "TRUNCATE then INSERT", one of which should be specified with "REFRESH_TYPE" field setting.

Following are the fields applicable for REFRESH command type:

| Field Name | Mandatory? | Description |
| ---------- | :-------: |-------------|
| CMD_SRC | Yes | Specify the name of source of data here. <p>This is usually a data table or a view that encapsulates the transformation business logic. <br>**Please include schema name with the object name e.g. [SCHEMA NAME].[OBJECT NAME] and all in CAPS please**</p> |
| CMD_TGT | Yes | Specify the name of target destination of data here. <p>This is usually a table. <br>**Please include schema name with the object name e.g. [SCHEMA NAME].[OBJECT NAME] and all in CAPS please**</p> |
| REFRESH_TYPE | Yes | Acceptable values are **DI/TI/OI** <p>**DI (Delete Insert)** - Before inserting the data in target, a delete command is run (where optionally filter clause can be added through CMD_WHERE).</p><p>**TI (Truncate Insert)** - Before inserting the data in target, truncate command on target is run.</p><p>**OI (Overwrite Insert)** - Before inserting the data in target, any existing data is removed from target. This works similar to truncate, with the caveat that TRUNCATE is a DDL command invoking a commit to the transaction where OVERWRITE doesn't commits the transaction immediately after delete, thus tables is rolled back to previous state if INSERT DML throws an error. |
| CMD_WHERE | No | This is where you can specify a filter clause that gets added to source as a WHERE clause at run time. <p>**WHERE** keyword should not be included and numbered bind variables can be used for which actual bind replacements are mentioned in CMD_BINDS <br>**E.g.</br><p>"C_MKTSEGMENT = :2 AND COBID = :1"**</p>In the above example values for bind variables are passed at run time, and derivation of value for bind variable according to the sequence is derived from CMD_BINDS</p> |
| CMD_BINDS | No | If bind variables are used in the step, here you specify the name for bind variables, delimited by Pipe **"\|"** symbol. <p>Bind variable values are passed in at runtime in a JSON format. Names of bind variables defined here are interpreted as keys from the variable values JSON object passed in at run-time</p> |
| ADDITIONAL_FIELDS | No | This is where you specify any additional columns/fields to be added to generated SELECT clause from source, which is not available in source. <p>E.g.</p><p>TO_NUMBER(:1) COBID</p><p>would add a column to generated SELECT statement, where it is transforming bind variable value passed in at run time, and aliased as COBID, which would then become part of INSERT/MERGE statement</p> |
| TEMP_TABLE | No | Acceptable values - Y/N/NULL <p>When set to Y, this would trigger creating a temporary table with the same name as target in the same schema as target before the operation of the step is run.</p><p>This feature utilises special functionality that has been introduced in Snowflake, that you can have permanent (or transient) table and a temporary table with the same name and temporary table is then given priority in the current running session.</p><p>In TiPS we utilise this functionality where a data pipeline can be run concurrently withing multiple sessions with its own bind variables and dataset are consistently transformed and published at session level</p> |

### TRUNCATE
This command type is for running TRUNCATE SQL command on the defined target.

Following are the fields applicable for TRUNCATE command type:

| Field Name | Mandatory? | Description |
| ---------- | :-------: |-------------|
| CMD_TGT | Yes | Specify the name of table from which data is to be deleted. <p>**Please include schema name with the object name e.g. [SCHEMA NAME].[OBJECT NAME] and all in CAPS please**</p> |
