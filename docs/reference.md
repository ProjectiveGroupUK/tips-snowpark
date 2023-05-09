# Reference Guide
## Metadata Tables
### PROCESS
This is the table where you define information about data pipeline e.g. Name of Pipeline and other information.

| Column Name | Description                     |
|-------------| ------------------------------- |
| PROCESS_ID | Sequentially generated ID.<p> This is defined as Auto increment on table, so doesn't needs to be included in DMLs</p> |
| PROCESS_NAME | Enter Name of Data Pipeline.<p> This value is passed as parameters when TiPS is executed. <br>**No whitespaces to be used in process name and preferably use Uppercase**</p> |
| PROCESS_DESCRIPTION | Description about Data Pipeline.<p> This is optional field, but good to have proper description about the pipeline for others to easily understand</p> |
| ACTIVE | Active (Y) / Inactive (N) flag.<p> When set to N (inactive), datapipeline can be disabled and TiPS will not execute it</p> |

### PROCESS_CMD
This is the table where you populate information about each steps in a data pipeline. There are several columns in this table, some of which are specific to command types. Out of those some are mandatory and some are optional. Further details about command types are documented [below](#command-types) 

| Column Name | Description                     |
|-------------| ------------------------------- |
| PROCESS_ID | Foreign Key to PROCESS_ID in PROCESS table<p>This needs to match ID of data pipeline defined in PROCESS table</p>|
| PROCESS_CMD_ID | Manually assigned sequential ID for Data Pipeline Step<p>When TiPS is executed, steps of a data pipeline run serially in sequential order. Order of the step is identified using this ID. It is advisable to leave gaps between the IDs when setting up the data pipeline initially, so that if there is a need to add an interim step later, it can be inserted in between without needing to reassign all IDs. E.g. 10,20,30,40... or 100,200,300,400... can be assigned initally which leave sufficient gap for steps to be inserted in between if need be, later. Also in a big team, where multiple data engineers might be working on same data pipeline in same development iteration (sprint), mechanism needs to be in place so that data engineers don't end up using overlapping IDs</p> |
| CMD_TYPE | This describes the operation type. Further details are given [below](#command-types) <p>E.g. APPEND / REFRESH / DELETE / TRUNCATE etc. <br>**All CAPS without spaces**</br></p>  |
| CMD_SRC | Specify the name of source of data here. <p>This is usually a data table or a view that encapculates the transformation business logic. <br>**Please include schema name with the object name e.g. [SCHEMA NAME].[OBJECT NAME] and all in CAPS please**</p> |
| CMD_TGT | Specify the name of target destination of data here. <p>This is usually a table. <br>**Please include schema name with the object name e.g. [SCHEMA NAME].[OBJECT NAME] and all in CAPS please**</p> |
| CMD_WHERE | This is where you can specify a filter clause that gets added to source as a WHERE clause at run time. <p>**WHERE** keyword should not be included and numbered bind variables can be used for which actual bind replacements are mentioned in CMD_BINDS <br>**E.g.</br><p>"C_MKTSEGMENT = :2 AND COBID = :1"**</p>In the above example values for bind variables are passed at run time, and derivation of value for bind variable according to the sequence is derived from CMD_BINDS</p> |
| CMD_BINDS | If bind variables are used in the step, here you specify the name for bind variables, delimitted by Pipe **"\|"** symbol. <p>Bind variable values are passed in at runtime in a JSON format. Names of bind variables defined here are interpretted as keys from the variable values JSON object passed in at run-time</p> |
| REFRESH_TYPE | This is only applicable for REFRESH command type. Acceptable values are **DI/TI/OI** <p>**DI (Delete Insert)** - Before inserting the data in target, a delete command is run (where optionally filter clause can be added through CMD_WHERE)</p><p>**TI (Truncate Insert)** - Before inserting the data in target, truncate command on target is run</p><p>**OI (Overwrite Insert)** - Before inserting the data in target, any existing data is removed from target. This works similar to truncate, with the caveat that TRUNCATE is a DDL command invoking a commit to the transaction where OVERWRITE doesn't commits the transaction immediately after delete, thus tables is rolled back to previous state if INSERT DML throws an error</p> |
| BUSINESS_KEY | This is only applicable for PUBLISH_SCD2_DIM command type. It is column(s delimitted by Pipe **"\|"** symbol) that defines a business key (also referred as natural key) for a dimension table. <p>For a slowly changing dimension, this is combination of key columns that uniquely identifies a row in the dimension (not a surrogate key), excluding record effective dates and/or current record flag</p> |
| MERGE_ON_FIELDS | This is only applicable for MERGE command type. Here you specify columns that are to be used in generated MERGE SQL in the **ON** join clause. Multiple fields delimitted by Pipe **"\|"** symbol |
| GENERATE_MERGE_MATCHED_CLAUSE | This is only applicable for MERGE command type. Here you specify whether ON MATCHED CLAUSE is to be generated in generated MERGE DML. Acceptaed values are Y/N. When Y is selected, ON MATCHED CLAUSE is generated which runs an UPDATE operation |
| GENERATE_MERGE_NON_MATCHED_CLAUSE | This is only applicable for MERGE command type. Here you specify whether ON NOT MATCHED CLAUSE is to be generated in generated MERGE DML. Acceptaed values are Y/N. When Y is selected, ON NOT MATCHED CLAUSE is generated which runs an INSERT operation|
| ADDITIONAL_FIELDS | This is where you specify any additional columns/fields to be added to generated SELECT clause from source, which is not available in source. <p>E.g.</p><p>TO_NUMBER(:1) COBID</p><p>would add a column to generated SELECT statement, where it is transforming bind variable value passed in at run time, and aliased as COBID, which would then become part of INSERT/MERGE statement</p> |
| TEMP_TABLE | Acceptable values - Y/N/NULL <p>When set to Y, this would trigger creating a temporary table with the same name as target in the same schema as target, before the operation of the step is run.</p><p>This feature utilises special functionality that has been introduced in snowflake, that you can have permanent(or transient) table and a temporary table with the same name and temporary table is then given priority in the current running session.</p><p>In TiPS we utilise this functionality where a data pipeline can be run concurrently withing multiple sessions with its own bind variables and dataset are consistently transformed and published at session level |
| CMD_PIVOT_BY | This is only applicable for COPY_INTO_FILE command type. If you are looking to apply a PIVOT in the SQL query in the COPY INTO statement then this can be used. This field will dictate which column's values are to be pivoted e.g. REPORTING_CCY |
| CMD_PIVOT_FIELD | This is only applicable for COPY_INTO_FILE command type. If you are looking to apply a PIVOT in the SQL query in the COPY INTO statement then this can be used. This field will be the aggregation of the field for the values which are included in the pivot e.g. SUM(REPORTING_CURRENCY) |
| ACTIVE | Active (Y) / Inactive (N) flag.<p> When set to N (inactive), datapipeline step can be disabled and TiPS will skip that step while execute the pipeline |
| FILE_FORMAT_NAME | This option is applicable to COPY_INTO_FILE and COPY_INTO_TABLE command types. <p>If a file format has been defined in the database, that can be used. <br>**Please include schema name with file format name e.g. [SCHEMA NAME].[FILE FORMAT NAME] and all in CAPS please**</p> |
| COPY_INTO_FILE_PARITITION_BY | This option is applicable to COPY_INTO_FILE command type. This adds PARTITION BY clause in generated COPY INTO FILE command. COPY_INTO_FILE_PARITITION_BY field needs to be an SQL expression that outputs a string. The dataset specified by CMD_SRC will then be split into individual files based on the output of the expression. A directory will be created in the stage specified by CMD_TGT which will be named the same as the partition clause. The data will then be output into this location in the stage. |

### PROCESS_LOG
This table holds logging information about each run of TiPS. This table is populated automatically at the end of execution of TiPS

### PROCESS_DQ_TEST
This table is populated with data relating to Data Quality Tests.

### PROCESS_CMD_TGT_DQ_TEST
This is the table that you populate with the information to enforce a predefined Data Quality test to a target (table) and additionally an attribute (column) 

### PROCESS_DQ_LOG
This table holds logging information about each Data Quality test execected withing a data pipeline when TiPS is run. This log is also associated to data in `PROCESS_LOG` table

## Command Types
### APPEND
This effectively generates a "INSERT INTO [target table] ([columns]) SELECT [columns] FROM [source] additionally WHERE", if applicable

Following are the fields applicable for APPEND command type:

| Field Name | Mandatory? | Description |
| ---------- | :-------: |-------------|
| CMD_SRC | Yes | Specify the name of source of data here. <p>This is usually a data table or a view that encapculates the transformation business logic. <br>**Please include schema name with the object name e.g. [SCHEMA NAME].[OBJECT NAME] and all in CAPS please**</p> |
| CMD_TGT | Yes | Specify the name of target destination of data here. <p>This is usually a table. <br>**Please include schema name with the object name e.g. [SCHEMA NAME].[OBJECT NAME] and all in CAPS please**</p> |
| CMD_WHERE | No | This is where you can specify a filter clause that gets added to source as a WHERE clause at run time. <p>**WHERE** keyword should not be included and numbered bind variables can be used for which actual bind replacements are mentioned in CMD_BINDS <br>**E.g.</br><p>"C_MKTSEGMENT = :2 AND COBID = :1"**</p>In the above example values for bind variables are passed at run time, and derivation of value for bind variable according to the sequence is derived from CMD_BINDS</p> |
| CMD_BINDS | No | If bind variables are used in the step, here you specify the name for bind variables, delimitted by Pipe **"\|"** symbol. <p>Bind variable values are passed in at runtime in a JSON format. Names of bind variables defined here are interpretted as keys from the variable values JSON object passed in at run-time</p> |
| ADDITIONAL_FIELDS | No | This is where you specify any additional columns/fields to be added to generated SELECT clause from source, which is not available in source. <p>E.g.</p><p>TO_NUMBER(:1) COBID</p><p>would add a column to generated SELECT statement, where it is transforming bind variable value passed in at run time, and aliased as COBID, which would then become part of INSERT/MERGE statement</p> |
| TEMP_TABLE | No | Acceptable values - Y/N/NULL <p>When set to Y, this would trigger creating a temporary table with the same name as target in the same schema as target, before the operation of the step is run.</p><p>This feature utilises special functionality that has been introduced in snowflake, that you can have permanent(or transient) table and a temporary table with the same name and temporary table is then given priority in the current running session.</p><p>In TiPS we utilise this functionality where a data pipeline can be run concurrently withing multiple sessions with its own bind variables and dataset are consistently transformed and published at session level</p> |

### COPY_INTO_FILE
This command type is for outputting data from a table/view into a file into an internal user stage or an internal named stage or an external stage

Following are the fields applicable for COPY_INTO_FILE command type:

| Field Name | Mandatory? | Description |
| ---------- | :-------: |-------------|
| CMD_SRC | Yes | Specify the name of source of data here. <p>This is usually a data table or a view that would provide data in desired form. <br>**Please include schema name with the object name e.g. [SCHEMA NAME].[OBJECT NAME] and all in CAPS please**</p> |
| CMD_TGT | Yes | This is the file location for outputting data from source to this file. If the PARTITION_CLAUSE is not being used then the CMD_TGT should be the exact path and filename that should be created. <p>If the PARTTION_CLAUSE is being used then this field should only contain a stage and a path.</p><p>For example, if the file should go into a user stage in the XYZ directory it should be @~/XYZ.</p><p>In both cases, it is permissible to use any BIND variables as part of the name i,e, @~/:1/XYZ would create a directory based on the first bind variable followed by XYZ.</p> |
| CMD_WHERE | No | This is where you can specify a filter clause that gets added to source as a WHERE clause at run time. <p>**WHERE** keyword should not be included and numbered bind variables can be used for which actual bind replacements are mentioned in CMD_BINDS <br>**E.g.</br><p>"C_MKTSEGMENT = :2 AND COBID = :1"**</p>In the above example values for bind variables are passed at run time, and derivation of value for bind variable according to the sequence is derived from CMD_BINDS</p> |
| CMD_BINDS | No | If bind variables are used in the step, here you specify the name for bind variables, delimitted by Pipe **"\|"** symbol. <p>Bind variable values are passed in at runtime in a JSON format. Names of bind variables defined here are interpretted as keys from the variable values JSON object passed in at run-time</p> |
| CMD_PIVOT_BY | No | If you are looking to apply a PIVOT in the SQL query in the COPY INTO statement then this can be used. This field will dictate which column's values are to be pivoted e.g. REPORTING_CCY |
| CMD_PIVOT_FIELD | No | If you are looking to apply a PIVOT in the SQL query in the COPY INTO statement then this can be used. This field will be the aggregation of the field for the values which are included in the pivot e.g. SUM(REPORTING_CURRENCY) |
| FILE_FORMAT_NAME | No | If a file format has been defined in the database with all applicable configurations, that this field can be used. <br>**Please include schema name with file format name e.g. [SCHEMA NAME].[FILE FORMAT NAME] and all in CAPS please**</br><p>If this field is omitted, then File Type "CSV" and compression "GZIP" is used by default |
| COPY_INTO_FILE_PARITITION_BY | No | This field is to be populated when you want to apply a PARTITION BY clause in generated COPY INTO FILE command. COPY_INTO_FILE_PARITITION_BY field needs to be an SQL expression that outputs a string. The dataset specified by CMD_SRC will then be split into individual files based on the output of the expression. A directory will be created in the stage specified by CMD_TGT which will be named the same as the partition clause.  The data will then be output into this location in the stage. |

### COPY_INTO_TABLE
### DELETE
### DQ_TEST
### MERGE
### PUBLISH_SCD2_DIM
### REFRESH
### TRUNCATE
