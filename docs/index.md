# Introduction to TiPS (Snowpark)

TiPS is acronym for "Transformation in Pure SQL".

TiPS is a simple to use Metadata driven transformation framework. All the metadata is stored in database tables in snowflake, which can easily be interogated using normal SQL commands.

Idealogy behind TiPS is that majority of the transformations can easily be coded using SQL langauge, which are easy to write and maintain by database professionals and there is literally no learning curve to get started with using TiPS.

TiPS works primarily on a source to target data movement, where source in majority of the cases are database view definitions encapsulating all the transformation business logic. Targets are database tables, consuming data from sources as either in the final state or in interim state of a data pipeline.

All TiPS objects are first class database objects.

When run in snowpark through stored procedure, TiPS provides an extra security feature where the executing user of the stored procedure doesn't need to have direct read/write priveleges on the underlying table/data. User calling the stored procedure only needs privileges to execute the stored procedure.

TiPS also supports in-line Data Quality support within data pipelines. Data Quality checks are also configurable through metadata.

## Data Pipeline Related Tables

* `PROCESS` - Holds information about Data Pipeline e.g. Name and Description of Data Pipeline.
* `PROCESS_CMD` - This table holds information about individual steps within a data pipeline.
* `PROCESS_LOG` - This table is populated with data pipeline execution logs when data pipelines are run through TiPS

## Data Quality Related Tables

* `PROCESS_DQ_TEST` - This table is shipped with some preconfigured DQ tests. New tests can be configured by the users themselves into this table.
* `PROCESS_CMD_TGT_DQ_TEST` - This table is configured with Linking DQ Tests to the Target (table).
* `PROCESS_DQ_LOG` - This table is populated with data quality test execution logs when data pipelines are run through TiPS. Data in this table is tied up to `PROCESS_LOG` table through  `process_log_id` column.
