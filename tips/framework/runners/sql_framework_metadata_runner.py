from typing import List, Dict
import json
from graphlib import TopologicalSorter

# from snowflake.snowpark import Session
from tips.framework.metadata.framework_metadata import FrameworkMetaData
from tips.framework.utils.sql_template import SQLTemplate
from tips.framework.utils.globals import Globals

# Below is to initialise logging
import logging
from tips.utils.logger import Logger

logger = logging.getLogger(Logger.getRootLoggerName())


class SQLFrameworkMetaDataRunner(FrameworkMetaData):
    _processName: str
    _processCmdId: int
    _globalsInstance: Globals

    def __init__(self, processName: str, processCmdID: int = 0) -> None:
        self._processName = processName
        self._processCmdId = processCmdID
        self._globalsInstance = Globals()

    def getMetaData(self) -> List[Dict]:
        logger.info("Fetching Framework Metadata...")
        session = self._globalsInstance.getSession()
        targetDatabase = self._globalsInstance.getTargetDatabase()

        cmdStr: str = SQLTemplate().getTemplate(
            sqlAction="framework_metadata",
            parameters={
                "process_name": self._processName,
                "process_cmd_id": self._processCmdId,
                "target_database": targetDatabase,
            },
        )

        results: List[Dict] = session.sql(cmdStr).collect()

        ## Apply topological sorting to the returned resultset if more than one row returned in resultset
        ## This is so that serial execution of steps of pipeline happen in correct order
        if len(results) > 1:
            graph = {}
            rows = {}
            for row in results:
                graph[row['PROCESS_CMD_ID']] = list(map(int, json.loads(row['PARENT_PROCESS_CMD_ID'])))
                rows[row['PROCESS_CMD_ID']] = row
            ts = TopologicalSorter(graph)
            sortedNodes = tuple(ts.static_order())
            sortedResult = []
            for node in sortedNodes:
                sortedResult.append(rows[node])

            results = sortedResult

        return results

    def getDQMetaData(self) -> Dict:
        logger.info("Fetching Framework DQ Metadata...")
        session = self._globalsInstance.getSession()
        targetDatabase = self._globalsInstance.getTargetDatabase()

        cmdStr: str = SQLTemplate().getTemplate(
            sqlAction="framework_dq_metadata",
            parameters={
                "process_name": self._processName,
                "process_cmd_id": self._processCmdId,
                "target_database": targetDatabase,
            },
        )

        results: List[Dict] = session.sql(cmdStr).collect()

        returnDict = {}
        scannedKeys = []
        for val in results:
            if val["PROCESS_CMD_ID"] not in scannedKeys:
                returnDict[val["PROCESS_CMD_ID"]] = []

            returnDict[val["PROCESS_CMD_ID"]].append(val)
            scannedKeys.append(val["PROCESS_CMD_ID"])
            scannedKeys = list(set(scannedKeys))

        return returnDict
