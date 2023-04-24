from typing import List, Dict
from snowflake.snowpark import Session
from tips.framework.metadata.framework_metadata import FrameworkMetaData
from tips.framework.utils.sql_template import SQLTemplate
# Below is to initialise logging
import logging
from tips.utils.logger import Logger
logger = logging.getLogger(Logger.getRootLoggerName())


class SQLFrameworkMetaDataRunner(FrameworkMetaData):

    _processName: str

    def __init__(self, processName) -> None:
        self._processName = processName

    def getMetaData(self, session: Session) -> List[Dict]:

        logger.info('Fetching Framework Metadata...')

        cmdStr: str = SQLTemplate().getTemplate(
            sqlAction="framework_metadata",
            parameters={"process_name": self._processName},
        )

        results: List[Dict] = session.sql(cmdStr).collect()
        return results

    def getDQMetaData(self, session: Session) -> Dict:

        logger.info('Fetching Framework DQ Metadata...')

        cmdStr: str = SQLTemplate().getTemplate(
            sqlAction="framework_dq_metadata",
            parameters={"process_name": self._processName},
        )

        results: List[Dict] = session.sql(cmdStr).collect()

        returnDict = {}
        scannedKeys = []
        for val in results:
            if val['PROCESS_CMD_ID'] not in scannedKeys:
                returnDict[val['PROCESS_CMD_ID']] = []

            returnDict[val['PROCESS_CMD_ID']].append(val)
            scannedKeys.append(val['PROCESS_CMD_ID'])
            scannedKeys = list(set(scannedKeys))

        return returnDict