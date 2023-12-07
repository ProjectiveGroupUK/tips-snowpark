"""
This script is to test the app through snowpark client running locally (not through stored procedure)
Example command to run this script
python run_process_local.py -p TIPS_TEST_PIPELINE -v "{'MARKET_SEGMENT':'FURNITURE', 'COBID':'20230127'}" -e N

It expects environment variables set for credentials. This can be achieved by creating .env file with following structure
SF_ACCOUNT=<<snowflake account>>
SF_USER=<<snowflake user>>
SF_PASSWORD=<<snowflake password>>
SF_ROLE=<<snowflake role>>
SF_WAREHOUSE=<<snowflake warehouse>>
SF_DATABASE=<<snowflake database>>
SF_SCHEMA=<<snowflake schema>>

**Change values in << >> as appropriate
"""
import os
from snowflake.snowpark import Session
from dotenv import load_dotenv
import argparse
from tips.framework.app import runWithTasksLocal as app_run
from tips.utils.logger import Logger
import json

logger = Logger().initialize()
# for logger_name in ('snowflake.snowpark', 'snowflake.connector'):
# logger = logging.getLogger('tips.framework.logger')
# logger.setLevel(logging.INFO)

load_dotenv()


connection_parameters = {
    "account": os.getenv('SF_ACCOUNT'),
    "user": os.getenv('SF_USER'),
    "password": os.getenv('SF_PASSWORD'),
    "role": os.getenv('SF_ROLE'),
    "warehouse": os.getenv('SF_WAREHOUSE'),
    "database": os.getenv('SF_DATABASE'),
    "schema": os.getenv('SF_SCHEMA')
}

session = Session.builder.configs(connection_parameters).create()
results = session.sql("call tips_md_schema.test()").collect()
print(results)
ret = json.loads(results[0]["TEST"])
print(type(ret))
print(ret)
# print(ret["STATUS"])
session.close()
