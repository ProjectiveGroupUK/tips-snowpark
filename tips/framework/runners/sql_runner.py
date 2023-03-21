from typing import Dict, List
from snowflake.snowpark import Session
from tips.framework.actions.action import Action
from tips.framework.actions.sql_action import SqlAction
from tips.framework.actions.sql_command import SQLCommand
from tips.framework.runners.runner import Runner
from datetime import datetime
# Below is to initialise logging
import logging
from tips.utils.logger import Logger

logger = logging.getLogger(Logger.getRootLoggerName())

class SQLRunner(Runner):

    def execute(self, action: Action, session: Session, frameworkRunner) -> None:
        commandList: List[object] = action.getCommands()
        ret: int = 0

        if commandList is None:
            pass
        else:
            for command in commandList:
                if isinstance(command, SQLCommand):
                    ret = self.executeSQL(command, session, frameworkRunner)
                    if ret == 1:
                        return ret
                elif isinstance(command, SqlAction):
                    self.execute(command, session, frameworkRunner)

            return ret

    def executeSQL(self, sql: SQLCommand, session: Session, frameworkRunner) -> int:

        sqlCommand:str = sql.getSqlCommand()
        logger.info(sqlCommand)

        if sql.getSqlBinds() is not None:
            cnt = 0
            binds = sql.getSqlBinds()
            for bind in binds:
                cnt += 1
                ## target and source replacement to be done without quotes, but all others shoud include quotes
                sqlCommand = sqlCommand.replace(f":{cnt}", f"{bind}") if sqlCommand is not None else None
                sqlCommand = sqlCommand.replace(f":'{cnt}'", f"'{bind}'") if sqlCommand is not None else None


        sqlJson: Dict = {
            "cmd_type": "SQL",
            "status": "NO EXECUTE" if frameworkRunner.isExecute() == False else "SUCCESS",
            "error_message": "",
            "sql_cmd": sqlCommand,
            "cmd_status": {
                # # "ROWS_INSERTED": 0,
                # # "ROWS_UPDATED": 0,
                # # "ROWS_DELETED" : 0,
                # # "ROWS_LOADED": 0,
                # # "ROWS_UNLOADED": 0,
                "STATUS": "NO EXECUTE" if frameworkRunner.isExecute() == False else "OK"
            }
        }

        if frameworkRunner.isExecute():
            try:
                dt1 = datetime.now()
                results:List[Dict] = session.sql(sqlCommand).collect()
                dt2 = datetime.now()
                timeDelta = dt2 - dt1
                sqlJson["cmd_status"]["EXECUTION_TIME_IN_SECS"] = round(timeDelta.total_seconds(),2)

                if type(results) == list and len(results) > 0:

                    for val in results:
                        if "number of rows deleted" in val:
                            sqlJson["cmd_status"]["ROWS_DELETED"] = val["number of rows deleted"]

                        if "number of rows inserted" in val:
                            sqlJson["cmd_status"]["ROWS_INSERTED"] = val["number of rows inserted"]

                        if "number of rows updated" in val:
                            sqlJson["cmd_status"]["ROWS_UPDATED"] = val["number of rows updated"]

                        if "rows_loaded" in val:
                            sqlJson["cmd_status"]["ROWS_LOADED"] = val["rows_loaded"]

                        if "rows_unloaded" in val:
                            sqlJson["cmd_status"]["ROWS_UNLOADED"] = val["rows_unloaded"]

                        if "status" in val:
                            sqlJson["cmd_status"]["STATUS"] = val["status"]
                        
                        if "CREATE_TEMPORARY_TABLE" in val:
                            sqlJson["cmd_status"]["STATUS"] = val["CREATE_TEMPORARY_TABLE"]

                    ## Now check if it was a query and there are any conditions to check
                    if sql.getSqlChecks() is not None and len(sql.getSqlChecks()) > 0:
                        for chk in sql.getSqlChecks():
                            for result in results:
                                sqlJson["cmd_status"]["CHECK_CONDITION"] = chk['condition']
                                if eval(f"result{chk['condition']}"):
                                    sqlJson["cmd_status"]["CHECK_CONDITION_STATUS"] = 'FAILED'
                                    raise ValueError(chk['error'])
                                else:
                                    sqlJson["cmd_status"]["CHECK_CONDITION_STATUS"] = 'PASSED'

            except Exception as err:
                sqlJson["status"] = "ERROR"
                sqlJson["error_message"] = str(err).replace("'","")
                sqlJson["cmd_status"]["STATUS"] = "ERROR"
                frameworkRunner.returnJson["steps"][-1]["commands"].append(sqlJson)                
                ##Also propogate higher in the heirarchy
                # frameworkRunner.returnJson["steps"][-1]["status"] = "ERROR"
                # frameworkRunner.returnJson["steps"][-1]["error_message"] = f'{err}'
                frameworkRunner.returnJson["status"] = "ERROR"
                frameworkRunner.returnJson["error_message"] = str(err).replace("'","")
                return 1

        frameworkRunner.returnJson["steps"][-1]["commands"].append(sqlJson)
        return 0
