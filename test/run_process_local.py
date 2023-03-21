"""
This script is to test the app through snowpark client running locally (not through stored procedure)
Example command to run this script
python run_process_local.py -p SAMPLE_CUSTOMER -v "{'MARKET_SEGMENT':'FURNITURE', 'COBID':'20230127'}" -e N

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
from tips.framework.app import runLocal as app_run
from tips.utils.logger import Logger

logger = Logger().initialize()
# for logger_name in ('snowflake.snowpark', 'snowflake.connector'):
# logger = logging.getLogger('tips.framework.logger')
# logger.setLevel(logging.INFO)

load_dotenv()

parser = argparse.ArgumentParser(
    usage="python app.py -p Process Name -v Variables in Dictionary Format -e Execute?(Y/N)",
    description="""E.g.: python run_process_local.py -p SAMPLE_CUSTOMER -v "{'MARKET_SEGMENT':'FURNITURE', 'COBID':'20230401'}" -e N""",
)

parser.add_argument(
    "-p",
    "--process",
    metavar="Process Name",
    dest="PROCESS_NAME",
    required=True,
    help="Process Name to run",
)
parser.add_argument(
    "-v",
    "--var",
    metavar="Bind Variables Dictionary",
    dest="VARS",
    help="Bind Variables to use in the run",
)
parser.add_argument(
    "-e",
    "--exec",
    metavar="Execute=Y/N",
    dest="EXEC",
    choices=["y", "Y", "n", "N"],
    required=True,
    help="Y - Execute pipeline, N - Run in Debug Mode",
)

args = parser.parse_args()

v_process_name = args.PROCESS_NAME.upper()

v_vars = args.VARS
if v_vars is not None:
    if v_vars.startswith("{") == False or v_vars.endswith("}") == False:
        raise ValueError(
            "Invalid value for argument Bind Variable. Should be in form of Dictionary!"
        )
    v_vars = v_vars.replace("'", '"')

v_exec = args.EXEC.upper()

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

response = app_run(session=session, process_name=v_process_name, vars=v_vars, execute_flag=v_exec, addLogFileHandler=True)
# response = app_run(session=session, process_name=v_process_name, vars=v_vars, execute_flag=v_exec)
logger.info('RUN_PROCESS_LOCAL finished!!')
session.close()
