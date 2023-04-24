# tips-snowpark
TiPS (Transformation in Pure SQL) is a transformation framework developed by ProjectiveGroup for Snowflake's snowpark for python environment

## NOTES:
----------------------------
When testing the changes through scripts in test folder (i.e. run_process_log.py and run_process_sp.py), you would need to set
you would need to set PYTHONPATH enviroment variable, with path that of your github folder e.g.
```
export PYTHONPATH=/c/GitHub/tips-snowpark
```
for relative paths in imports to work

And then from the test folder you can run a command, e.g.
```
python run_process_local.py -p SAMPLE_CUSTOMER -v "{'BIND_VAR_KEY1':'BIND_VAR_KEY1', 'BIND_VAR_KEY2':'BIND_VAR_KEY2'}" -e N
```

In the above command, SAMPLE_CUSTOMER is the data pipeline name, as defined in process table. Additionally command binds variables can be passed with -v argument, if used in the process. Last argument -e can accept Y/N, indicating whether generated sqls are to be executed or just outputted.

----------------------------
Once you are happy with the changes and are ready to upload the code to snowflake stage, you would need to 
zip the content of tips folder and upload the zip file.

Zip files can be uploaded using snowsql, which can be downloaded and installed from snowflake's website https://developers.snowflake.com/snowsql/

When snowsql is installed, it creates a .snowsql folder under user's home folder, which contains a config file. It would be preferable 
to set user credentials into config file, so that user credentials are not required everytime with snowsql. Below is an example block of 
user credentials that can be added to config file
```
[connections.tips_user]
accountname = dtsquaredpartner [Change to the appropriate account name]
region = eu-west-1 [Change to the appropriate region name]
username = tips_user [Change to the appropriate user name]
password = *********** [Change astericks to textual password]
dbname = elt_framework [Change to the appropriate database]
schemaname = tips_md_schema [Change to the appropriate schema]
warehousename = demo_wh [Change to the appropriate warehouse name]
rolename = sysadmin [Change to the appropriate role name]
```

Once the credentials have been set, you can connect to snowsql with the following command:
```
snowsql -c tips_user
```
And then following command can be run from within snowsql to upload the zip file to the named stage
* Please make changes to below command as appropriate for your setup *
```
put file:///GitHub/tips-snowpark/tips.zip @tips auto_compress=false overwrite=true;
```
After uploading the zip file to named stage, you need to compile 2 stored procedures (available inside stored_procedure_stub folder) in your database:
1) create_temporary_table.sql
2) run_process_sp.sql

Once these 2 scripts are compiled in your environment, you are good to execute run_process_sp stored procedure, passing in appropriate parameters, to run data pipelines.

