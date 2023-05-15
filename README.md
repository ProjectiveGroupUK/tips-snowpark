# Introduction to TiPS (Snowpark)

### What is TiPS?
TiPS is a simple data transformation and data quality framework built for Snowflake.

The ideology behind TiPS was to create a framework that an experienced database professional, already adept with SQL, could easily deliver data pipelines with virtually zero learning curve.

A data pipeline in TiPS is made up multiple steps that run serially, with each step performing it's own operation to move data from source to target or checking data quality. Steps within a data pipeline can perform one of two things:

* A movement of data from a source to a target. In most cases the sources are database views encapsulating transformation logic in the desired form, while the targets are database tables.
* A data quality check to make sure data being moved from source to target conforms to the desired form, before getting consumed by the data consumer and thus providing inconsistent results.

An Example Data Pipeline in TiPS:
![Process Cmd Table Example](docs/images/process_cmd.png)

TiPS was built with security in mind. The database credentials used to execute a pipeline do not require read/write access to the underlying data, in this regard TiPS differs from other data transformation tools. We believe that data pipelines should be idempotent, this being the case if pipeline execution credentials were ever leaked the worst-case scenario is that a pipeline could be re-executed (compute costs would increase but data integrity would not be compromised).

### What TiPS is not?
**TiPS is not a scheduler/orchestrator tool:**
<br>TiPS doesn't have any scheduling or orchestration capabilities built in. Orchestrating or scheduling for execution of data pipelines on a regular interval, can be done through other tools like Airflow, Control-M or Unix Cron for that matter.

**<p>TiPS is not a Data Ingestion tool:**
<br>TiPS is a transformation framework, and is not placed to be a replacement for data ingestion tools like Fivetran, Matillion etc. With TiPS, usually the starting source of data for the data pipeline is, either data already landed into Snowflake tables from source, or from files stored in a Snowflake accessible stage (external or internal, E.g. S3 on AWS).

### How does TiPS work?

TiPS is a simple to use Metadata driven transformation framework. All the metadata is stored in database tables in Snowflake, which can easily be interrogated using normal SQL commands.

All TiPS objects are first class database objects

When run in Snowpark through stored procedure, TiPS provides an extra security feature where the executing user of the stored procedure doesn't need to have direct read/write privileges on the underlying table/data. User calling the stored procedure only needs privileges to execute the stored procedure.

### TiPS Metadata Tables:

![TiPS ERD](docs/images/tips_erd.png)


## Getting Started
Refer to the online [documentation](https://projectivegroupuk.github.io/tips-snowpark/getting-started/) for getting started.

## Reference Guide
Refer to the online [documentation](https://projectivegroupuk.github.io/tips-snowpark/reference/) for reference guide.

**Complete documentation is available [here](https://projectivegroupuk.github.io/tips-snowpark/)**