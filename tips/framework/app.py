import time
import os
import sys
import json
from uuid import uuid4
from typing import Dict, List
from snowflake.snowpark import Session
from tips.framework.factories.framework_factory import FrameworkFactory
from tips.framework.metadata.column_metadata import ColumnMetadata
from tips.framework.metadata.table_metadata import TableMetaData
from tips.framework.metadata.framework_metadata import FrameworkMetaData
from tips.framework.runners.framework_runner import FrameworkRunner
from tips.framework.utils.globals import Globals

# from tips.framework.utils.logger import Logger
from datetime import datetime

# Below is to initialise logging
import logging
from tips.utils.logger import Logger

logger = logging.getLogger(Logger.getRootLoggerName())
globalsInstance = Globals()
globalsInstance.setCallerId(callerId="None")


class App:
    _processName: str
    _bindVariables: Dict
    _executeFlag: str
    _session: Session
    _addLogFileHandler: bool
    _processCmdId: int
    _runID: str
    _warehouseName: str

    def __init__(
        self,
        session: Session,
        processName: str,
        bindVariables: str,
        executeFlag: str,
        addLogFileHandler: bool = False,
        targetDatabaseName: str = None,
        processCmdId: int = 0,
        runID: str = None,
        warehouseName: str = None,
    ) -> None:
        self._session = session
        self._processName = processName
        self._bindVariables = (
            dict()
            if bindVariables is None
            else json.loads(bindVariables)
            if type(bindVariables) == str
            else bindVariables
        )
        self._executeFlag = executeFlag
        self._addLogFileHandler = addLogFileHandler
        globalsInstance.setSession(session=self._session)
        if targetDatabaseName is not None:
            globalsInstance.setTargetDatabase(targetDatabase=targetDatabaseName)
        else:
            targetDBName = self._session.sql(
                "SELECT CURRENT_DATABASE() AS CURR_DB"
            ).collect()[0]["CURR_DB"]
            globalsInstance.setTargetDatabase(targetDatabase=targetDBName)

        self._processCmdId = processCmdId
        self._warehouseName = warehouseName

        if runID is None:
            self._runID = uuid4().hex
        else:
            self._runID = runID

        # currRole = self._session.sql("SELECT CURRENT_ROLE() AS CURR_ROLE").collect()[0]["CURR_ROLE"]
        # raise ValueError(currRole)

    def main(self) -> None:
        logger.debug("Inside framework app main")

        runFramework: dict = {}  ##Just initialise it

        if self._addLogFileHandler:
            Logger().addFileHandler(processName=self._processName)

        try:
            start_dt = datetime.now()

            if self._processCmdId > 0:
                framework: FrameworkMetaData = FrameworkFactory().getProcessStep(
                    processName=self._processName, processCmdId=self._processCmdId
                )
            else:
                framework: FrameworkMetaData = FrameworkFactory().getProcess(
                    processName=self._processName
                )

            frameworkMetaData: List[Dict] = framework.getMetaData()

            if len(frameworkMetaData) <= 0:
                err = "Could not fetch Metadata. Please make sure correct process name is passed and metadata setup has been done correctly first!"
                logger.error(err)
                runFramework["status"] = "ERROR"
                runFramework["error_message"] = err
            else:
                logger.info("Fetched Framework Metadata!")
                processStartTime = start_dt

                frameworkDQMetaData: List[Dict] = framework.getDQMetaData()

                columnMetaData: List[Dict] = ColumnMetadata().getData(
                    frameworkMetaData=frameworkMetaData
                )

                tableMetaData: TableMetaData = TableMetaData(columnMetaData)

                frameworkRunner: FrameworkRunner = FrameworkRunner(
                    processName=self._processName,
                    bindVariables=self._bindVariables,
                    executeFlag=self._executeFlag,
                )

                runFramework, dqTestLogs = frameworkRunner.run(
                    tableMetaData=tableMetaData,
                    frameworkMetaData=frameworkMetaData,
                    frameworkDQMetaData=frameworkDQMetaData,
                )

                if self._addLogFileHandler:
                    Logger().writeResultJson(runFramework)

                # Now insert process run log to database
                processEndTime = datetime.now()
                results = self._session.sql(
                    "SELECT TIPS_MD_SCHEMA.PROCESS_LOG_SEQ.NEXTVAL AS SEQVAL"
                ).collect()
                seqVal = results[0]["SEQVAL"]

                logJsonString = (
                    json.dumps(runFramework)
                    .replace("'", "''")
                    .replace("\\n", " ")
                    .replace("\\r", "")
                    .replace('\\"', "''")
                )

                sqlCommand = f"""
    INSERT INTO tips_md_schema.process_log (run_id, process_log_id, process_name, process_start_time, process_end_time, process_elapsed_time_in_seconds, execute_flag, status, error_message, log_json)
    SELECT '{self._runID}', {seqVal}, '{self._processName}','{processStartTime}','{processEndTime}',{round((processEndTime - processStartTime).total_seconds(),2)},'{self._executeFlag}','{runFramework["status"]}','{runFramework["error_message"]}',PARSE_JSON('{logJsonString}')
                """
                # logger.info(sqlCommand)
                results = self._session.sql(sqlCommand).collect()

                # Now insert DQ Logs if any
                if len(dqTestLogs) > 0:
                    for dqTestLog in dqTestLogs:
                        if len(dqTestLog) > 0 and dqTestLog != {}:
                            sqlCommand = f"""
        INSERT INTO tips_md_schema.process_dq_log (
          run_id
        , process_log_id
        , tgt_name
        , attribute_name
        , dq_test_name
        , dq_test_query
        , dq_test_result
        , start_time
        , end_time
        , elapsed_time_in_seconds
        , status
        , status_message
        )
        SELECT '{self._runID}' 
             , {seqVal}
             , '{dqTestLog["tgt_name"]}'
             , '{dqTestLog["attribute_name"]}'
             , '{dqTestLog["dq_test_name"]}'
             , '{dqTestLog["dq_test_query"].replace("'","''")}'
             , PARSE_JSON('{json.dumps(dqTestLog["dq_test_result"]).replace("'","''")}')
             , '{dqTestLog["start_time"]}'
             , '{dqTestLog["end_time"]}'
             , '{dqTestLog["elapsed_time_in_seconds"]}'
             , '{dqTestLog["status"]}'
             , '{dqTestLog["status_message"]}'
                            """
                            results = self._session.sql(sqlCommand).collect()

            end_dt = datetime.now()
            logger.info(f"Start DateTime: {start_dt}")
            logger.info(f"End DateTime: {end_dt}")
            logger.info(
                f"Total Elapsed Time (secs): {round((end_dt - start_dt).total_seconds(),2)}"
            )

            if runFramework.get("status") == "ERROR":
                raise ValueError(runFramework.get("error_message"))
            elif runFramework.get("status") == "WARNING":
                warning_message = runFramework.get("warning_message")
                logger.warning(warning_message)

            return runFramework
        except Exception as ex:
            sys.tracebacklimit = (
                None if os.environ.get("env", "dev") == "dev" else 0
            )  ## Only show error trace in dev environment
            logger.error(ex)
            raise
        finally:
            if self._addLogFileHandler:
                Logger().removeFileHandler()

    def runWithTasks(self) -> None:
        logger.debug("Inside framework app runWithTasks")
        targetDatabase = globalsInstance.getTargetDatabase()

        runFramework: dict = {
            "status": "NO EXECUTE" if self._executeFlag != "Y" else "SUCCESS",
            "error_message": str(),
            "warning_message": str(),
            "session_variables": self._bindVariables,
            "process": self._processName,
            "run_id": self._runID,
            "execute": self._executeFlag,
            "steps": [],
        }

        if self._addLogFileHandler:
            Logger().addFileHandler(processName=self._processName)

        try:
            if self._processCmdId > 0:
                framework: FrameworkMetaData = FrameworkFactory().getProcessStep(
                    processName=self._processName, processCmdId=self._processCmdId
                )
            else:
                framework: FrameworkMetaData = FrameworkFactory().getProcess(
                    processName=self._processName
                )

            frameworkMetaData: List[Dict] = framework.getMetaData()

            if len(frameworkMetaData) <= 0:
                err = "Could not fetch Metadata. Please make sure correct process name is passed and metadata setup has been done correctly first!"
                logger.error(err)
                runFramework["status"] = "ERROR"
                runFramework["error_message"] = err
            else:
                logger.info("Fetched Framework Metadata!")
                frameworkDQMetaData: List[Dict] = framework.getDQMetaData()

                taskCounter: int = 0
                vars = json.dumps(self._bindVariables)

                currentWareHouse = self._warehouseName

                transientTableList: list = []
                taskDict: dict = {}  ## This would be needed later to enable tasks

                # Loop through the metadata and setup tasks
                for fwMetaData in frameworkMetaData:
                    fwMetaData = fwMetaData.as_dict()
                    processCmdId = fwMetaData["PROCESS_CMD_ID"]
                    targetTable = fwMetaData["CMD_TGT"]
                    tempTableFlag = "Y" if fwMetaData["TEMP_TABLE"] == "Y" else "N"
                    taskName = f"{self._processName}_{processCmdId}_{self._runID}"
                    warehouseSize = fwMetaData["WAREHOUSE_SIZE"]
                    parentProcessCmdId = fwMetaData["PARENT_PROCESS_CMD_ID"]
                    if parentProcessCmdId is not None:
                        parentProcessCmdId = json.loads(parentProcessCmdId)

                    if warehouseSize is None or warehouseSize == "":
                        warehouseToUse = currentWareHouse
                    else:
                        lst = [
                            x for x in currentWareHouse.split("_")[:-1]
                        ]  ##this removes the text after last underscore
                        lst.append(warehouseSize)
                        warehouseToUse = "_".join(lst)

                    if taskCounter == 0:
                        # Root task
                        rootTaskName = taskName
                        if globalsInstance.isNotCalledFromNativeApp():
                            sqlCommand = f"""
CREATE TASK {taskName}
WAREHOUSE={warehouseToUse}
SCHEDULE='11520 MINUTE'
AS
DECLARE
  retJSON VARIANT;
  errorMessage VARCHAR;
  errorReturned EXCEPTION (-20001, 'run_process_step for command id {processCmdId} returned error. Please check logs for further details!');
  run_id VARCHAR := '{self._runID}';
  temp_table_flag VARCHAR := '{tempTableFlag}';
  target_table VARCHAR := '{targetTable}';
  temp_table_array VARIANT := ARRAY_CONSTRUCT();
  execute_flag VARCHAR := '{self._executeFlag}';
BEGIN
  retJSON := (call run_process_step('{self._processName}', {processCmdId}, '{vars}', '{self._executeFlag}', '{self._runID}'));
  errorMessage := NULLIF(retJSON:error_message,'');

  IF (errorMessage = NULL) THEN
    RAISE errorReturned;
  ELSE
    --Only execute below in Execute Mode
    IF (temp_table_flag = 'Y' AND execute_flag = 'Y') THEN
        EXECUTE IMMEDIATE 'CREATE OR REPLACE TRANSIENT TABLE '||target_table||'_'||run_id||' CLONE '||target_table;
    END IF;
  END IF;
END;
"""
                            results = self._session.sql(sqlCommand).collect()
                        else:
                            sqlCommand = f"call tips_md_schema.setup_process_tasks('{targetDatabase}','CREATE_TASK', OBJECT_CONSTRUCT('root_task',TRUE,'task_name','{taskName}','warehouse','{warehouseToUse}','process_name','{self._processName}','process_cmd_id','{processCmdId}','run_id','{self._runID}','temp_table_flag','{tempTableFlag}','target_table','{targetTable}','execute_flag','{self._executeFlag}','vars','{vars}'))"
                            results = self._session.sql(sqlCommand).collect()
                            ret = json.loads(results[0]["SETUP_PROCESS_TASKS"])
                            if ret["STATUS"] == "ERROR":
                                raise ValueError(ret["ERROR_MESSAGE"])

                    else:
                        parentTaskList = ""
                        for parentCmdId in parentProcessCmdId:
                            if parentTaskList == "":
                                if globalsInstance.isNotCalledFromNativeApp():
                                    parentTaskList = f"{self._processName}_{parentCmdId}_{self._runID}"
                                else:
                                    parentTaskList = f"transform.{self._processName}_{parentCmdId}_{self._runID}"
                            else:
                                parentTaskList = f"{parentTaskList}, {self._processName}_{parentCmdId}_{self._runID}"

                        if globalsInstance.isNotCalledFromNativeApp():
                            sqlCommand = f"""
CREATE TASK {taskName}
WAREHOUSE={warehouseToUse}
AFTER {parentTaskList}
AS
DECLARE
  retJSON VARIANT;
  errorMessage VARCHAR;
  errorReturned EXCEPTION (-20001, 'run_process_step for command id {processCmdId} returned error. Please check logs for further details!');
  run_id VARCHAR := '{self._runID}';
  temp_table_flag VARCHAR := '{tempTableFlag}';
  target_table VARCHAR := '{targetTable}';
  temp_table_array VARIANT := PARSE_JSON('{str(transientTableList).replace("'", '"')}');
  temp_table_cnt INTEGER;
  execute_flag VARCHAR := '{self._executeFlag}';
BEGIN
  --If there are element in transientTableList, then create the temp version of those
  temp_table_cnt := ARRAY_SIZE(temp_table_array);
  --Only execute below in Execute Mode
  IF (temp_table_cnt > 0 AND execute_flag = 'Y') THEN
    FOR i in 0 TO temp_table_cnt-1 LOOP
        LET transient_table_name := GET(temp_table_array,i)::VARCHAR;
        LET temp_table_name := REPLACE(transient_table_name,'_{self._runID}');
        EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE '||temp_table_name||' CLONE '||transient_table_name;
    END LOOP;
  END IF;
  retJSON := (call run_process_step('{self._processName}', {processCmdId}, '{vars}', '{self._executeFlag}','{self._runID}'));
  errorMessage := NULLIF(retJSON:error_message,'');

  IF (errorMessage = NULL) THEN
    RAISE errorReturned;
  ELSE
    --Only execute below in Execute Mode
    IF (temp_table_flag = 'Y' AND execute_flag = 'Y') THEN
        LET transient_table_name := target_table||'_'||run_id;
        EXECUTE IMMEDIATE 'CREATE OR REPLACE TRANSIENT TABLE '||transient_table_name||' CLONE '||target_table;
    END IF;
  END IF;
END;
"""
                            results = self._session.sql(sqlCommand).collect()
                        else:
                            transTableList = str(transientTableList).replace("'", '"')
                            sqlCommand = f"call tips_md_schema.setup_process_tasks('{targetDatabase}','CREATE_TASK', OBJECT_CONSTRUCT('root_task',FALSE,'task_name','{taskName}','warehouse','{warehouseToUse}','after','{parentTaskList}','process_name','{self._processName}','process_cmd_id','{processCmdId}','run_id','{self._runID}','temp_table_flag','{tempTableFlag}','target_table','{targetTable}','execute_flag','{self._executeFlag}','vars','{vars}','transient_table_list','{transTableList}'))"
                            results = self._session.sql(sqlCommand).collect()
                            ret = json.loads(results[0]["SETUP_PROCESS_TASKS"])
                            if ret["STATUS"] == "ERROR":
                                raise ValueError(ret["ERROR_MESSAGE"])

                    taskCounter += 1
                    # previousTaskName = taskName
                    taskDict[processCmdId] = taskName
                    if tempTableFlag == "Y":
                        transientTableList.append(f"{targetTable}_{self._runID}")
                    logger.info(f"Created task with Task ID {taskName}")

                ## If tasks have been created then enable all tasks and start executing the tasks
                if len(taskDict) > 0:
                    # Create an additional task, which drops all transient tables created in the process
                    lastProcessCmdId = list(taskDict.keys())[-1]
                    processCmdId = lastProcessCmdId + 10
                    taskName = f"{self._processName}_{processCmdId}_{self._runID}"
                    if globalsInstance.isNotCalledFromNativeApp():
                        parentTaskList = (
                            f"{self._processName}_{lastProcessCmdId}_{self._runID}"
                        )
                    else:
                        parentTaskList = f"transform.{self._processName}_{lastProcessCmdId}_{self._runID}"
                    warehouseToUse = currentWareHouse
                    if globalsInstance.isNotCalledFromNativeApp():
                        sqlCommand = f"""
CREATE TASK {taskName}
WAREHOUSE={warehouseToUse}
AFTER {parentTaskList}
AS
DECLARE
  temp_table_array VARIANT := PARSE_JSON('{str(transientTableList).replace("'", '"')}');
  temp_table_cnt INTEGER;
  execute_flag VARCHAR := '{self._executeFlag}';
BEGIN
  --If there are element in transientTableList, then drop those
  temp_table_cnt := ARRAY_SIZE(temp_table_array);
  --Only execute below in Execute Mode
  IF (temp_table_cnt > 0 AND execute_flag = 'Y') THEN
    FOR i in 0 TO temp_table_cnt-1 LOOP
        LET transient_table_name := GET(temp_table_array,i)::VARCHAR;
        EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS '||transient_table_name;
    END LOOP;
  END IF;
END;
"""
                        results = self._session.sql(sqlCommand).collect()
                    else:
                        transTableList = str(transientTableList).replace("'", '"')
                        sqlCommand = f"call tips_md_schema.setup_process_tasks('{targetDatabase}','CLEANUP_TASK_TABLES', OBJECT_CONSTRUCT('root_task',FALSE,'task_name','{taskName}','warehouse','{warehouseToUse}','after','{parentTaskList}','transient_table_list','{transTableList}','execute_flag','{self._executeFlag}'))"
                        results = self._session.sql(sqlCommand).collect()
                        ret = json.loads(results[0]["SETUP_PROCESS_TASKS"])
                        if ret["STATUS"] == "ERROR":
                            raise ValueError(ret["ERROR_MESSAGE"])

                    taskDict[processCmdId] = taskName

                    if globalsInstance.isNotCalledFromNativeApp():
                        sqlCommand = (
                            f"SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('{rootTaskName}')"
                        )
                        results = self._session.sql(sqlCommand).collect()
                    else:
                        sqlCommand = f"call tips_md_schema.setup_process_tasks('{targetDatabase}','ENABLE_ROOT_TASK', OBJECT_CONSTRUCT('task_name','{rootTaskName}'))"
                        results = self._session.sql(sqlCommand).collect()
                        ret = json.loads(results[0]["SETUP_PROCESS_TASKS"])
                        if ret["STATUS"] == "ERROR":
                            raise ValueError(ret["ERROR_MESSAGE"])

                    if globalsInstance.isNotCalledFromNativeApp():
                        sqlCommand = f"EXECUTE TASK {rootTaskName}"
                        results = self._session.sql(sqlCommand).collect()
                    else:
                        sqlCommand = f"call tips_md_schema.setup_process_tasks('{targetDatabase}','EXECUTE_ROOT_TASK', OBJECT_CONSTRUCT('task_name','{rootTaskName}'))"
                        results = self._session.sql(sqlCommand).collect()
                        ret = json.loads(results[0]["SETUP_PROCESS_TASKS"])
                        if ret["STATUS"] == "ERROR":
                            raise ValueError(ret["ERROR_MESSAGE"])

                    logger.info("Started execution of Tasks...")

                    
                    while True:
                        logger.debug("Waiting for execution of tasks to finish...")
                        if globalsInstance.isNotCalledFromNativeApp():
                            sqlCommand = f"""
select state, first_error_task_name, first_error_message
from table({targetDatabase}.information_schema.complete_task_graphs (
    root_task_name=>'{rootTaskName}'))
"""
                            results = self._session.sql(sqlCommand).collect()
                        else:
                            ## With Native App, we cannot query table functions in information, so we have to instead rely
                            ## on process log information
                            # sqlCommand = f"call tips_md_schema.setup_process_tasks('{targetDatabase}','QUERY_TASK_STATUS', OBJECT_CONSTRUCT('task_name','{rootTaskName}'))"
                            # results = self._session.sql(sqlCommand).collect()
                            # ret = json.loads(results[0]["SETUP_PROCESS_TASKS"])
                            # raise ValueError(ret)
                            # if ret["STATUS"] == "ERROR":
                            #     raise ValueError(ret["ERROR_MESSAGE"])
                            # else:
                            #     results = ret["SQL_RETURN"]
                            sqlCommand = f"""
SELECT COUNT(*) AS total_cnt
     , NVL(SUM(DECODE(STATUS,'ERROR',1,0)),0) AS cnt_error
     , ARRAY_AGG(DECODE(STATUS,'ERROR',log_json,NULL)) WITHIN GROUP (ORDER BY process_log_id) AS error_log_json
  FROM tips_md_schema.process_log WHERE run_id = '{self._runID}'
"""
                            results = self._session.sql(sqlCommand).collect()
                            if len(results) > 0:
                                totalTaskRunCount = results[0]["TOTAL_CNT"]
                                totalTaskErrorCount = results[0]["CNT_ERROR"]
                                errorLogJson = json.loads(results[0]["ERROR_LOG_JSON"])
                                if totalTaskErrorCount > 0:
                                    errorMessage = errorLogJson[0]["error_message"]
                                    results = [
                                        {
                                            "STATE": "FAILED",
                                            "FIRST_ERROR_MESSAGE": errorMessage,
                                        }
                                    ]
                                else:
                                    if (totalTaskRunCount == taskCounter):
                                        results = [{"STATE": "SUCCEEDED"}]
                                    else:
                                        results = []

                        if len(results) > 0:
                            if results[0]["STATE"] == "SUCCEEDED":
                                logger.info(
                                    f"All Tasks completed Successfully, please check log for run ID {self._runID} for further details"
                                )

                                # Also remove all tasks
                                # First suspend the root task before dropping
                                if globalsInstance.isNotCalledFromNativeApp():
                                    results = self._session.sql(
                                        f"ALTER TASK {rootTaskName} SUSPEND"
                                    ).collect()
                                else:
                                    sqlCommand = f"call tips_md_schema.setup_process_tasks('{targetDatabase}','SUSPEND_ROOT_TASK', OBJECT_CONSTRUCT('task_name','{rootTaskName}'))"
                                    results = self._session.sql(sqlCommand).collect()
                                    ret = json.loads(results[0]["SETUP_PROCESS_TASKS"])
                                    if ret["STATUS"] == "ERROR":
                                        raise ValueError(ret["ERROR_MESSAGE"])

                                for v in taskDict.values():
                                    if globalsInstance.isNotCalledFromNativeApp():
                                        results = self._session.sql(
                                            f"DROP TASK {v}"
                                        ).collect()
                                    else:
                                        sqlCommand = f"call tips_md_schema.setup_process_tasks('{targetDatabase}','DROP_TASK', OBJECT_CONSTRUCT('task_name','{v}'))"
                                        results = self._session.sql(
                                            sqlCommand
                                        ).collect()
                                        ret = json.loads(
                                            results[0]["SETUP_PROCESS_TASKS"]
                                        )
                                        if ret["STATUS"] == "ERROR":
                                            raise ValueError(ret["ERROR_MESSAGE"])

                                # Now also fetch Log information from the database
                                sqlCommand = f"SELECT ARRAY_AGG(log_json) WITHIN GROUP (ORDER BY process_log_id) AS log_json FROM tips_md_schema.process_log WHERE run_id = '{self._runID}'"
                                results = self._session.sql(sqlCommand).collect()
                                logReturn = json.loads(results[0]["LOG_JSON"])
                                # runFramework["steps"] = logReturn
                                # Now also loop through the steps to check for any error or warning and propogate it up to root level
                                for stepLog in logReturn:
                                    runFramework["steps"].extend(stepLog["steps"])
                                    if stepLog["error_message"] != "":
                                        ##Below is to capture only the first instance of error
                                        if runFramework["status"] != "ERROR":
                                            runFramework["status"] = "ERROR"
                                            runFramework["error_message"] = stepLog[
                                                "error_message"
                                            ]
                                    elif stepLog["warning_message"] != "":
                                        if runFramework["status"] != "ERROR":
                                            runFramework["status"] = "WARNING"
                                            runFramework["warning_message"] = stepLog[
                                                "warning_message"
                                            ]

                                break
                            elif results[0]["STATE"] == "FAILED":
                                errorMsg = results[0]["FIRST_ERROR_MESSAGE"]
                                logger.error(
                                    f"Task execution failed on task, please check log for run ID {self._runID} for further details"
                                )
                                logger.error(f"Error Reported: {errorMsg}")
                                logger.error(
                                    "All db objects created are left behind, please clean up manually..."
                                )
                                runFramework["status"] = "ERROR"
                                runFramework["error_message"] = errorMsg
                                break

                        time.sleep(1)

            if runFramework.get("status") == "ERROR":
                raise ValueError(runFramework.get("error_message"))
            elif runFramework.get("status") == "WARNING":
                warning_message = runFramework.get("warning_message")
                logger.warning(warning_message)

            return runFramework

        except Exception as ex:
            sys.tracebacklimit = (
                None if os.environ.get("env", "dev") == "dev" else 0
            )  ## Only show error trace in dev environment
            logger.error(ex)
            raise
        finally:
            if self._addLogFileHandler:
                Logger().writeResultJson(runFramework)
                Logger().removeFileHandler()


def run(
    session,
    process_name: str,
    vars: str,
    execute_flag: str,
) -> Dict:
    globalsInstance.setCallerId(callerId="Snowpark")
    app = App(
        session=session,
        processName=process_name,
        bindVariables=vars,
        executeFlag=execute_flag,
        addLogFileHandler=False,
        targetDatabaseName=None,
    )
    response: Dict = app.main()
    return response


def runProcessWithTasks(
    session,
    process_name: str,
    vars: str,
    execute_flag: str,
) -> Dict:
    globalsInstance.setCallerId(callerId="Snowpark")
    warehouseName = session.sql("SELECT CURRENT_WAREHOUSE() AS CURR_WH").collect()[0][
        "CURR_WH"
    ]
    app = App(
        session=session,
        processName=process_name,
        bindVariables=vars,
        executeFlag=execute_flag,
        addLogFileHandler=False,
        targetDatabaseName=None,
        warehouseName=warehouseName,
    )
    response: Dict = app.runWithTasks()
    return response


def runProcessStep(
    session,
    process_name: str,
    process_cmd_id: int,
    vars: str,
    execute_flag: str,
    run_id: str,
) -> Dict:
    globalsInstance.setCallerId(callerId="Snowpark")
    app = App(
        session=session,
        processName=process_name,
        bindVariables=vars,
        executeFlag=execute_flag,
        addLogFileHandler=False,
        targetDatabaseName=None,
        processCmdId=process_cmd_id,
        runID=run_id,
    )
    response: Dict = app.main()
    return response


def run_sf_native_app(
    session,
    process_name: str,
    vars: str,
    execute_flag: str,
    targetDatabaseName: str,
) -> Dict:
    globalsInstance.setCallerId(callerId="NativeApp")
    app = App(
        session=session,
        processName=process_name,
        bindVariables=vars,
        executeFlag=execute_flag,
        addLogFileHandler=False,
        targetDatabaseName=targetDatabaseName,
        warehouseName=None,
    )
    response: Dict = app.main()
    return response


def run_process_step_sf_native_app(
    session,
    process_name: str,
    process_cmd_id: int,
    vars: str,
    execute_flag: str,
    targetDatabaseName: str,
    run_id: str,
) -> Dict:
    globalsInstance.setCallerId(callerId="NativeApp")
    app = App(
        session=session,
        processName=process_name,
        bindVariables=vars,
        executeFlag=execute_flag,
        addLogFileHandler=False,
        targetDatabaseName=targetDatabaseName,
        processCmdId=process_cmd_id,
        runID=run_id,
    )
    response: Dict = app.main()
    return response


def run_process_with_tasks_sf_native_app(
    session,
    process_name: str,
    vars: str,
    execute_flag: str,
    targetDatabaseName: str,
    warehouseName: str,
) -> Dict:
    globalsInstance.setCallerId(callerId="NativeApp")
    app = App(
        session=session,
        processName=process_name,
        bindVariables=vars,
        executeFlag=execute_flag,
        addLogFileHandler=False,
        targetDatabaseName=targetDatabaseName,
        warehouseName=warehouseName,
    )
    response: Dict = app.runWithTasks()
    return response


def runLocal(
    session,
    process_name: str,
    vars: str,
    execute_flag: str,
    addLogFileHandler: bool = False,
) -> Dict:
    app = App(
        session=session,
        processName=process_name,
        bindVariables=vars,
        executeFlag=execute_flag,
        addLogFileHandler=addLogFileHandler,
    )
    response: Dict = app.main()
    return response


def runWithTasksLocal(
    session,
    process_name: str,
    vars: str,
    execute_flag: str,
    addLogFileHandler: bool = False,
) -> Dict:
    globalsInstance.setCallerId(callerId="Snowpark")
    warehouseName = session.sql("SELECT CURRENT_WAREHOUSE() AS CURR_WH").collect()[0][
        "CURR_WH"
    ]
    app = App(
        session=session,
        processName=process_name,
        bindVariables=vars,
        executeFlag=execute_flag,
        addLogFileHandler=addLogFileHandler,
        warehouseName=warehouseName,
    )
    response: Dict = app.runWithTasks()
    return response


def runProcessStepLocal(
    session,
    process_name: str,
    vars: str,
    execute_flag: str,
    process_cmd_id: int,
    addLogFileHandler: bool = False,
) -> Dict:
    app = App(
        session=session,
        processName=process_name,
        bindVariables=vars,
        executeFlag=execute_flag,
        addLogFileHandler=addLogFileHandler,
        processCmdId=process_cmd_id,
    )
    response: Dict = app.main()
    return response
