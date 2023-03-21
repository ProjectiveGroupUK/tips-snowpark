"""
This script is to test the app through snowpark client running via python stored procedure in snowflake
Example command to run this script
python run_process_sp.py -p SAMPLE_CUSTOMER -v "{'MARKET_SEGMENT':'FURNITURE', 'COBID':'20230401'}" -e N

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
import logging
from tips.utils.logger import Logger

logger = Logger().initialize()

# logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
# ch = logging.StreamHandler()
# ch.setLevel(logging.DEBUG)
# logger.addHandler(ch)

# for logger_name in ('snowflake.snowpark', 'snowflake.connector', 'tips.framework.logger'): 
#     logger = logging.getLogger(__name__)
#     logger.setLevel(logging.INFO)
#     ch = logging.StreamHandler()
#     ch.setLevel(logging.INFO)
#     ch.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s %(filename)s:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s'))
#     logger.addHandler(ch)


load_dotenv()

parser = argparse.ArgumentParser(
    usage="python app.py -p Process Name -v Variables in Dictionary Format -e Execute?(Y/N)",
    description="""E.g.: python app.py -p PUBLISH_CUSTOMER -v "{'MARKET_SEGMENT':'FURNITURE', 'COBID':'20210401'}" -e N""",
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

response = session.sql(f"call run_process_sp('{v_process_name}','{v_vars}', '{v_exec}')").collect()

logger.info('RUN_PROCESS_SP completed!!')

session.close()
