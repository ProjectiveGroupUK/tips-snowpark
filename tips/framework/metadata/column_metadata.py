import json
import re
from typing import List, Dict
from snowflake.snowpark import Session
from itertools import groupby

from tips.framework.metadata.column_info import ColumnInfo

# Below is to initialise logging
import logging
from tips.utils.logger import Logger

logger = logging.getLogger(Logger.getRootLoggerName())


class ColumnMetadata:
    def getData(
        self, frameworkMetaData: List[Dict], session: Session
    ) -> List[Dict]:

        try:

            def key_func(k):
                return k["schema_name"] + "." + k["table_name"]

            logger.info("Fetching Column Metadata...")

            schemas = set()
            data: List[Dict] = list()
            returnColumnMetaData: Dict[str, List[ColumnInfo]] = dict()

            pkData: Dict[str, List[str]] = dict()
            seqData: List = list()

            cmdStr = f"SELECT CURRENT_DATABASE() AS DB"
            results: List[Dict] = session.sql(cmdStr).collect()
            databaseName:str = results[0]['DB']

            for val in frameworkMetaData:
                # For all cmd_src
                schemaName = (val["CMD_SRC"] if val["CMD_SRC"] is not None else '').split(".", 1)[0]
                ## Schema name start with alpha or underscore and only contains alphanumeric, underscore or dollar
                if re.match("^[a-zA-Z_]+.", schemaName) and re.match(
                    "^[\w_$]+$", schemaName
                ):
                    schemas.add(schemaName)

                # For all cmd_tgt
                schemaName = (val["CMD_TGT"] if val["CMD_TGT"] is not None else '').split(".", 1)[0]
                ## Schema name start with alpha or underscore and only contains alphanumeric, underscore or dollar
                if re.match("^[a-zA-Z_]+.", schemaName) and re.match(
                    "^[\w_$]+$", schemaName
                ):
                    schemas.add(schemaName)

            for schemaName in schemas:
                cmdStr = f"SHOW COLUMNS IN SCHEMA {databaseName}.{schemaName}"
                results = session.sql(cmdStr).collect()

                for result in results:
                    data.append(result)

                ## Also fetch table_names on these schemas, if they have primary key defined
                cmdStr = f"""SELECT table_schema||'.'||table_name AS table_name
                                FROM information_schema.table_constraints 
                                WHERE table_catalog = '{databaseName}'
                                AND table_schema = '{schemaName}'
                                AND constraint_type = 'PRIMARY KEY'"""

                results = session.sql(cmdStr).collect()

                if len(results) > 0:
                    for result in results:
                        pkData[result["TABLE_NAME"]] = list()

                ## And also fetch sequences in the schema
                cmdStr = f"""SELECT sequence_schema||'.'||sequence_name AS sequence_name
                                FROM information_schema.sequences 
                                WHERE sequence_catalog = '{databaseName}'
                                AND sequence_schema = '{schemaName}'"""

                results = session.sql(cmdStr).collect()

                if len(results) > 0:
                    for result in results:
                        seqData.append(result["SEQUENCE_NAME"])

            # Now loop through PK data and populate column list. This has to be done in 2 passes, as column information is only available in
            # DESC table command
            for key in pkData:
                cmdStr = f"DESC TABLE {key}"
                results: List[Dict] = session.sql(cmdStr).collect()

                cmdStr = f"""SELECT "name" as column_name
                               FROM table(result_scan(last_query_id()))
                              WHERE "kind" = 'COLUMN'
                                AND "primary key" = 'Y'"""

                results: List[Dict] = session.sql(cmdStr).collect()

                for result in results:
                    pkData[key].append(result["COLUMN_NAME"])

            data = sorted(data, key=key_func)

            for key, value in groupby(data, key=key_func):
                tbl: List[ColumnInfo] = list()

                schemaName = key.split(".", 1)[0]
                tableName = key.split(".", 1)[1]
                for row in value:
                    columnName = row["column_name"]
                    dataType = json.loads(row["data_type"])["type"]
                    isVirtual = row["kind"] == "VIRTUAL_COLUMN"

                    if key in pkData and columnName in pkData[key]:
                        isPK = True
                    else:
                        isPK = False

                    if (
                        columnName.endswith("_KEY")
                        or columnName.endswith("_ID")
                        or columnName.endswith("_SEQ")
                    ):
                        if f"{schemaName}.SEQ_{tableName}" in seqData:
                            sequenceName = f"{schemaName}.SEQ_{tableName}"
                        else:
                            sequenceName = None
                    else:
                        sequenceName = None

                    tbl.append(
                        ColumnInfo(columnName, dataType, isVirtual, isPK, sequenceName)
                    )

                returnColumnMetaData[key] = tbl

            logger.info("Fetched Column Metadata!")
          
            return returnColumnMetaData

        except:
            logging.error(f"Error: Fetching Column Metadata")
            raise