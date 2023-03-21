import json
from typing import Dict, List
from snowflake.snowpark import Session
from tips.framework.factories.framework_factory import FrameworkFactory
from tips.framework.metadata.column_metadata import ColumnMetadata
from tips.framework.metadata.table_metadata import TableMetaData
from tips.framework.metadata.framework_metadata import FrameworkMetaData
from tips.framework.runners.framework_runner import FrameworkRunner

from datetime import datetime

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

    def __init__(self, session: Session, processName: str, bindVariables: str, executeFlag: str, addLogFileHandler: bool) -> None:
        self._session = session
        self._processName = processName
        self._bindVariables = (
            dict() if bindVariables is None else json.loads(bindVariables)
        )
        self._executeFlag = executeFlag
        self._addLogFileHandler = addLogFileHandler

    def main(self) -> Dict:
        logger.debug("Inside framework app main")
        logInstance = Logger()

        if self._addLogFileHandler:
            logInstance.addFileHandler()

        logger.debug("DB Connection established!")

        start_dt = datetime.now()
        framework: FrameworkMetaData = FrameworkFactory().getProcess(self._processName)
        frameworkMetaData: List[Dict] = framework.getMetaData(self._session)

        if len(frameworkMetaData) <= 0:
            logger.error(
                "Could not fetch Metadata. Please make sure correct process name is passed and metadata setup has been done correctly first!"
            )
        else:
            logger.info("Fetched Framework Metadata!")
            processStartTime = start_dt

            columnMetaData: List[Dict] = ColumnMetadata().getData(
                frameworkMetaData=frameworkMetaData, session=self._session
            )

            tableMetaData: TableMetaData = TableMetaData(columnMetaData)

            frameworkRunner: FrameworkRunner = FrameworkRunner(
                processName=self._processName,
                bindVariables=self._bindVariables,
                executeFlag=self._executeFlag,
            )

            runFramework: Dict = frameworkRunner.run(
                session=self._session,
                tableMetaData=tableMetaData,
                frameworkMetaData=frameworkMetaData,
            )

            if self._addLogFileHandler:
                logInstance.writeResultJson(runFramework)

            # Now insert process run log to database
            processEndTime = datetime.now()
            results = self._session.sql("SELECT TIPS_MD_SCHEMA.PROCESS_LOG_SEQ.NEXTVAL AS SEQVAL FROM DUAL").collect()
            seqVal = results[0]["SEQVAL"]

            sqlCommand = f"""
INSERT INTO tips_md_schema.process_log (process_log_id, process_name, process_start_time, process_end_time, process_elapsed_time_in_seconds, execute_flag, status, error_message, log_json)
SELECT {seqVal}, '{self._processName}','{processStartTime}','{processEndTime}',{round((processEndTime - processStartTime).total_seconds(),2)},'{self._executeFlag}','{runFramework["status"]}','{runFramework["error_message"]}',PARSE_JSON('{json.dumps(runFramework).replace("'","''")}')
            """
            # logger.info(sqlCommand)
            results = self._session.sql(sqlCommand).collect()


            if runFramework.get("status") == "ERROR":
                error_message = runFramework.get("error_message")
                logger.error(error_message)

        if self._addLogFileHandler:
            logInstance.removeFileHandler()

        end_dt = datetime.now()
        logger.info(f"Start DateTime: {start_dt}")
        logger.info(f"End DateTime: {end_dt}")
        logger.info(
            f"Total Elapsed Time (secs): {round((end_dt - start_dt).total_seconds(),2)}"
        )

        return runFramework


def run(session, process_name: str, vars: str, execute_flag: str) -> Dict:
    app = App(session=session, processName=process_name, bindVariables=vars,
              executeFlag=execute_flag, addLogFileHandler=False)
    response: Dict = app.main()
    return response


def runLocal(session, process_name: str, vars: str, execute_flag: str, addLogFileHandler: bool = False) -> Dict:
    app = App(session=session, processName=process_name, bindVariables=vars,
              executeFlag=execute_flag, addLogFileHandler=addLogFileHandler)
    response: Dict = app.main()
    return response
