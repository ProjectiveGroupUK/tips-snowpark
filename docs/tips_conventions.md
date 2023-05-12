# TiPS Conventions
## Must Know
### Columns in Generated DMLs
TiPS automatically identifies common columns between Source and Target, that are then used in generated DMLs. Hence, columns that are needed to be part of generated DMLs in TiPS, should be present in both source and target and with the same name. In select statement of views, aliases can be used where applicable.

## Must Follow
### SCD (Slow Changing Dimension) Type 2
When using PUBLISH_SCD2_DIM command type for populating SCD Type2, please make sure you following columns are included in Dimension Table

- EFFECTIVE_START_DATE with datatype DATE, can additionally have NOT NULL constraint.
- EFFECTIVE_END_DATE with datatype DATE, can additionally have NOT NULL constraint.
- IS_CURRENT_ROW with datatype BOOLEAN, can additionally have NOT NULL constraint.
- BINARY_CHECK_SUM with datatype NUMBER. This is a virtual column that contains hashed value of columns that are to be checked for changed values. <p>E.g.</p><p>`BINARY_CHECK_SUM NUMBER (38,0) AS (HASH(CUSTOMER_NAME, CUSTOMER_ADDRESS, CUSTOMER_PHONE, CUSTOMER_MARKET_SEGMENT, CUSTOMER_COMMENT, COUNTRY, REGION))
`  

View/Table that is the source for Dimension, should only have EFFECTIVE_START_DATE, and this should be populated with the date that the new record should be effective from. 

### MERGE - Key/ID Column to be populated from Sequence
Where a target table needs to have Key/ID column that has to be populated from a sequence, please make sure that column name ends with `_KEY` or `_SEQ`. And also make sure that SEQUENCE name starts with `SEQ_` followed by target table name and is created in same schema as that of target table.

### Schema Names prepended to Objects
In `PROCESS_CMD` metadata table, wherever database objects are referenced, please make sure to include the schema name E.g. `[SCHEMA NAME].[TABLE NAME]`

### Upper Case for DB Objects
In `PROCESS_CMD` metadata table, wherever database objects are referenced, please make sure to enter object names (including schema name), all in UPPERCASE.

### No White Spaces in Process Name
In `PROCESS` table, or wherever process name is referenced, don't include any white spaces in the process name. Instead, underscore can be used to indicate a logical split between words.

## Good to Follow
* `NOT NULL` constraint should be used, where possible.
* It is a good practise, not to allow NULL values in tables in presentation layer (accessible by end user). If NULL values are likely to be encountered from source systems, a sensible replacement of null values should be used as a standard.
* For text fields, standards around casing (uppercase or lowercase or any other as preferred) should be adopted when data is populated in presentation layer tables (or tables accessed by end user).
