from typing import Dict, List

from tips.framework.actions.action import Action
from tips.framework.actions.sql_action import SqlAction
from tips.framework.actions.sql_command import SQLCommand
from tips.framework.runners.runner import Runner
from datetime import datetime
from tips.framework.utils.globals import Globals

# Below is to initialise logging
import logging
from tips.utils.logger import Logger

logger = logging.getLogger(Logger.getRootLoggerName())


class SQLRunner(Runner):
    def execute(self, action: Action, frameworkRunner) -> int:
        commandList: List[object] = action.getCommands()
        executeReturn: int = 0
        dqTestAbortSignal: bool = False

        if commandList is None:
            return 0
        else:
            for command in commandList:
                if isinstance(command, SQLCommand):
                    ##For DQ Test we are going to run all the test and capture if there is any one
                    ##with error and abort
                    ret, dqTestAbort = self.executeSQL(command, frameworkRunner)
                    if ret == 1:
                        executeReturn = 1
                        break
                    if dqTestAbort:
                        dqTestAbortSignal = True

                elif isinstance(command, SqlAction):
                    ret = self.execute(command, frameworkRunner)
                    if ret == 1:
                        executeReturn = 1
                        break

            ## If any one of the DQ Test had error and abort, then we want the process to stop after
            ## runing all the tests in that step, hence returning 1
            if dqTestAbortSignal:
                return 1
            else:
                return executeReturn

    def executeSQL(self, sql: SQLCommand, frameworkRunner) -> int:
        globalsInstance = Globals()
        session = globalsInstance.getSession()
        sqlExecutionSequence = globalsInstance.getSQLExecutionSequence() + 1
        globalsInstance.setSQLExecutionSequence(
            sqlExecutionSequence=sqlExecutionSequence
        )

        dqTestAbort: bool = False
        sqlCommand: str = sql.getSqlCommand()
        
        dqLog: dict = {}

        if sql.getSqlBinds() is not None:
            cnt = 0
            binds = sql.getSqlBinds()
            for bind in binds:
                cnt += 1
                # If it is DQ Test, it would not have bind variables in source and target, so it needs to be handled differently
                if sql.getDQCheckDict() is not None:
                    sqlCommand = (
                        sqlCommand.replace(f":{cnt}", f"'{bind}'")
                        if sqlCommand is not None
                        else None
                    )
                else:
                    ## target and source replacement to be done without quotes, but all others shoud include quotes
                    sqlCommand = (
                        sqlCommand.replace(f":{cnt}", f"{bind}")
                        if sqlCommand is not None
                        else None
                    )
                    sqlCommand = (
                        sqlCommand.replace(f":'{cnt}'", f"'{bind}'")
                        if sqlCommand is not None
                        else None
                    )

        logger.info(sqlCommand)

        sqlJson: Dict = {
            "cmd_type": "SQL",
            "cmd_sequence": sqlExecutionSequence,
            "status": "NO EXECUTE"
            if frameworkRunner.isExecute() == False
            else "SUCCESS",
            "error_message": "",
            "sql_cmd": sqlCommand,
            "cmd_status": {
                "STATUS": "NO EXECUTE" if frameworkRunner.isExecute() == False else "OK"
            },
        }

        if frameworkRunner.isExecute():
            try:
                dt1 = datetime.now()
                results = session.sql(sqlCommand).collect()
                dt2 = datetime.now()
                timeDelta = dt2 - dt1
                sqlJson["cmd_status"]["EXECUTION_TIME_IN_SECS"] = round(
                    timeDelta.total_seconds(), 2
                )

                if type(results) == list and len(results) > 0:
                    for val in results:
                        if "number of rows deleted" in val:
                            sqlJson["cmd_status"]["ROWS_DELETED"] = val[
                                "number of rows deleted"
                            ]

                        if "number of rows inserted" in val:
                            sqlJson["cmd_status"]["ROWS_INSERTED"] = val[
                                "number of rows inserted"
                            ]

                        if "number of rows updated" in val:
                            sqlJson["cmd_status"]["ROWS_UPDATED"] = val[
                                "number of rows updated"
                            ]

                        if "rows_loaded" in val:
                            sqlJson["cmd_status"]["ROWS_LOADED"] = val["rows_loaded"]

                        if "rows_unloaded" in val:
                            sqlJson["cmd_status"]["ROWS_UNLOADED"] = val[
                                "rows_unloaded"
                            ]

                        if "status" in val:
                            sqlJson["cmd_status"]["STATUS"] = val["status"]

                """ 
                If it is a DQ test command then:
                    1. log DQ result in its log table
                    2. Handle error or warning as defined
                """
                if sql.getDQCheckDict() is not None:
                    dqCheckDict = sql.getDQCheckDict()
                    dqLog["tgt_name"] = dqCheckDict["TGT_NAME"]
                    dqLog["attribute_name"] = dqCheckDict["ATTRIBUTE_NAME"]
                    dqLog["dq_test_name"] = dqCheckDict["PROCESS_DQ_TEST_NAME"]
                    dqLog["dq_test_query"] = sqlCommand
                    dqLog["dq_test_result"] = results
                    dqLog["start_time"] = dt1
                    dqLog["end_time"] = dt2
                    dqLog["elapsed_time_in_seconds"] = sqlJson["cmd_status"][
                        "EXECUTION_TIME_IN_SECS"
                    ]

                    if len(results) > 0:
                        if dqCheckDict["ERROR_AND_ABORT"]:
                            dqLog["status"] = "ERROR"
                            dqLog["status_message"] = dqCheckDict["DQ_ERROR_MESSAGE"]

                            # dqLog[
                            #     "status_message"
                            # ] = "DQ Test Failed with Error, process aborted!"

                            sqlJson["status"] = "ERROR"
                            sqlJson["error_message"] = dqLog["status_message"]
                            sqlJson["cmd_status"]["STATUS"] = "ERROR"
                            ##Also propogate higher in the heirarchy
                            frameworkRunner.returnJson["steps"][-1]["status"] = "ERROR"
                            frameworkRunner.returnJson["steps"][-1][
                                "error_message"
                            ] = dqLog["status_message"]
                            frameworkRunner.returnJson["status"] = "ERROR"
                            frameworkRunner.returnJson[
                                "error_message"
                            ] = "DQ Test Failed with Error, process aborted!"

                            dqTestAbort = True  ##Process Abort Indicator

                        else:
                            dqLog["status"] = "WARNING"
                            dqLog["status_message"] = dqCheckDict["DQ_ERROR_MESSAGE"]
                            # dqLog[
                            #     "status_message"
                            # ] = "Some of the DQ test(s) failed with warning, please check logs for more details!"

                            sqlJson["status"] = "WARNING"
                            sqlJson["warning_message"] = dqLog["status_message"]
                            sqlJson["cmd_status"]["STATUS"] = "WARNING"
                            ##Also propogate higher in the heirarchy, if there is not already an ERROR reported
                            if (
                                frameworkRunner.returnJson["steps"][-1]["status"]
                                != "ERROR"
                            ):
                                frameworkRunner.returnJson["steps"][-1][
                                    "status"
                                ] = "WARNING"
                                frameworkRunner.returnJson["steps"][-1][
                                    "warning_message"
                                ] = dqLog["status_message"]

                            if frameworkRunner.returnJson["status"] != "ERROR":
                                frameworkRunner.returnJson["status"] = "WARNING"
                                frameworkRunner.returnJson[
                                    "warning_message"
                                ] = "Some of the DQ test(s) failed with warning, please check logs for more details!"

                    else:
                        dqLog["status"] = "PASSED"
                        dqLog["status_message"] = None

                    frameworkRunner.dqTestLogList.append(dqLog)

                ## Now check if it was a query and there are any conditions to check
                if sql.getSqlChecks() is not None and len(sql.getSqlChecks()) > 0:
                    for chk in sql.getSqlChecks():
                        for result in results:
                            sqlJson["cmd_status"]["CHECK_CONDITION"] = chk["condition"]
                            if result["CNT"] > 0:
                                sqlJson["cmd_status"][
                                    "CHECK_CONDITION_STATUS"
                                ] = "FAILED"
                                raise ValueError(chk["error"])
                            else:
                                sqlJson["cmd_status"][
                                    "CHECK_CONDITION_STATUS"
                                ] = "PASSED"

            except Exception as err:
                sqlJson["status"] = "ERROR"
                sqlJson["error_message"] = str(err).replace("'", "")
                sqlJson["cmd_status"]["STATUS"] = "ERROR"
                frameworkRunner.returnJson["steps"][-1]["commands"].append(sqlJson)
                ##Also propogate higher in the heirarchy
                frameworkRunner.returnJson["status"] = "ERROR"
                frameworkRunner.returnJson["error_message"] = str(err).replace("'", "")
                frameworkRunner.dqTestLogList.append(dqLog)
                return 1, dqTestAbort

        frameworkRunner.returnJson["steps"][-1]["commands"].append(sqlJson)

        return 0, dqTestAbort
