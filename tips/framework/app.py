import json
import re
from typing import Dict, List
from snowflake.snowpark import Session
from tips.framework.factories.framework_factory import FrameworkFactory
from tips.framework.metadata.column_metadata import ColumnMetadata
from tips.framework.metadata.table_metadata import TableMetaData
from tips.framework.metadata.framework_metadata import FrameworkMetaData
from tips.framework.runners.framework_runner import FrameworkRunner

# from tips.framework.utils.logger import Logger
from datetime import datetime
import argparse

# Below is to initialise logging
import logging
from tips.utils.logger import Logger

logger = logging.getLogger(Logger.getRootLoggerName())


class App:
    _processName: str
    _bindVariables: Dict
    _executeFlag: str
    _session: Session
    _addLogFileHandler: bool

    def __init__(
        self,
        session: Session,
        processName: str,
        bindVariables: str,
        executeFlag: str,
        addLogFileHandler: bool = False,
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

    def main(self) -> None:
        logger.debug("Inside framework app main")

        if self._addLogFileHandler:
            Logger().addFileHandler(processName=self._processName)

        try:
            start_dt = datetime.now()
            framework: FrameworkMetaData = FrameworkFactory().getProcess(
                self._processName
            )
            frameworkMetaData: List[Dict] = framework.getMetaData(session=self._session)

            if len(frameworkMetaData) <= 0:
                logger.error(
                    "Could not fetch Metadata. Please make sure correct process name is passed and metadata setup has been done correctly first!"
                )
            else:
                logger.info("Fetched Framework Metadata!")
                processStartTime = start_dt

                frameworkDQMetaData: List[Dict] = framework.getDQMetaData(
                    session=self._session
                )

                columnMetaData: List[Dict] = ColumnMetadata().getData(
                    frameworkMetaData=frameworkMetaData, session=self._session
                )

                tableMetaData: TableMetaData = TableMetaData(columnMetaData)

                frameworkRunner: FrameworkRunner = FrameworkRunner(
                    processName=self._processName,
                    bindVariables=self._bindVariables,
                    executeFlag=self._executeFlag,
                )

                runFramework, dqTestLogs = frameworkRunner.run(
                    session=self._session,
                    tableMetaData=tableMetaData,
                    frameworkMetaData=frameworkMetaData,
                    frameworkDQMetaData=frameworkDQMetaData,
                )
                Logger().writeResultJson(runFramework)

                # Now insert process run log to database
                processEndTime = datetime.now()
                results = self._session.sql(
                    "SELECT TIPS_MD_SCHEMA.PROCESS_LOG_SEQ.NEXTVAL AS SEQVAL FROM DUAL"
                ).collect()
                seqVal = results[0]["SEQVAL"]

                logJsonString = (
                    json.dumps(runFramework)
                    .replace("'", "''")
                    .replace("\\n", " ")
                    .replace("\\r", "")
                )

                sqlCommand = f"""
    INSERT INTO tips_md_schema.process_log (process_log_id, process_name, process_start_time, process_end_time, process_elapsed_time_in_seconds, execute_flag, status, error_message, log_json)
    SELECT {seqVal}, '{self._processName}','{processStartTime}','{processEndTime}',{round((processEndTime - processStartTime).total_seconds(),2)},'{self._executeFlag}','{runFramework["status"]}','{runFramework["error_message"]}',PARSE_JSON('{logJsonString}')
                """
                # logger.info(sqlCommand)
                results = self._session.sql(sqlCommand).collect()

                # Now insert DQ Logs if any
                if len(dqTestLogs) > 0:
                    for dqTestLog in dqTestLogs:
                        if len(dqTestLog) > 0 and dqTestLog != {}:
                            sqlCommand = f"""
        INSERT INTO tips_md_schema.process_dq_log (
            process_log_id
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
        SELECT {seqVal}
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

        except Exception as ex:
            logger.error(ex)
            raise
        finally:
            Logger().removeFileHandler()


def run(session, process_name: str, vars: str, execute_flag: str) -> Dict:
    app = App(
        session=session,
        processName=process_name,
        bindVariables=vars,
        executeFlag=execute_flag,
        addLogFileHandler=False,
    )
    response: Dict = app.main()
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
