from typing import List

from tips.framework.actions.sql_action import SqlAction
from tips.framework.actions.sql_command import SQLCommand
from tips.framework.utils.sql_template import SQLTemplate
from tips.framework.utils.globals import Globals


class CopyIntoFileAction(SqlAction):
    _source: str
    _target: str
    _whereClause: str
    _binds: List[str]
    _pivotBy: str
    _pivotField: str
    _fileFormatName: str
    _copyIntoFilePartitionBy: str
    _selectQuery: str

    def __init__(
        self,
        source: str,
        target: str,
        whereClause: str,
        binds: List[str],
        pivotBy: str,
        pivotField: str,
        fileFormatName: str,
        copyIntoFilePartitionBy: str,
    ) -> None:
        self._source = source
        self._target = target
        self._whereClause = whereClause
        self._binds = binds
        self._pivotBy = pivotBy
        self._pivotField = pivotField
        self._fileFormatName = fileFormatName
        self._copyIntoFilePartitionBy = copyIntoFilePartitionBy

    def getBinds(self) -> List[str]:
        return self._binds

    def getCommands(self) -> List[object]:
        retCmd: List[object] = []
        globalsInstance = Globals()
        session = globalsInstance.getSession()
        targetDatabase = globalsInstance.getTargetDatabase()

        ## append quotes with bind variable
        cnt = 0
        while True:
            cnt += 1
            if self._whereClause is not None and f":{cnt}" in self._whereClause:
                self._whereClause = (
                    self._whereClause.replace(f":{cnt}", f"':{cnt}'")
                    if self._whereClause is not None
                    else None
                )
            else:
                break

        if self._pivotBy is not None and self._pivotField is not None:
            # generate the distinct list of pivot fields
            cmdStr = f"SELECT LISTAGG(DISTINCT ''''||{self._pivotBy}||'''',',') AS PIVOT_FIELD_LIST FROM {self._source}"
            results = session.sql(cmdStr).collect()
            pivotFieldList: str = results[0]["PIVOT_FIELD_LIST"]

            self._selectQuery = f"SELECT * FROM {self._source} PIVOT({self._pivotField} FOR {self._pivotBy} IN ({pivotFieldList}))"
        else:
            self._selectQuery = f"SELECT * FROM {self._source}"

        ##Transpose stage name in filename with database and schema namespace
        if self._target.startswith("@"):
            slashPosition = self._target.find("/")
            if slashPosition == -1:  ##No slash(folder path exists)
                stageName = self._target[1:]
            else:
                stageName = self._target[1:slashPosition]

            ##Now check if there are any dots already. If there are 2 then we don't need to do anything
            if stageName.count(".") == 2:
                targetName = self._target
            elif stageName.count(".") == 1:
                if slashPosition == -1:
                    targetName = f"@{targetDatabase}.{stageName}"
                else:
                    targetName = (
                        f"@{targetDatabase}.{stageName}{self._target[slashPosition:]}"
                    )
            elif stageName.count(".") == 0:
                currentSchema = session.sql(
                    "SELECT CURRENT_SCHEMA() AS CURR_SCHEMA"
                ).collect()[0]["CURR_SCHEMA"]
                if slashPosition == -1:
                    targetName = f"@{targetDatabase}.{currentSchema}.{stageName}"
                else:
                    targetName = f"@{targetDatabase}.{currentSchema}.{stageName}{self._target[slashPosition:]}"
            else:
                targetName = self._target
        else:
            targetName = self._target

        cmd: str = SQLTemplate().getTemplate(
            sqlAction="copy_into_file",
            parameters={
                "fileName": targetName,
                "selectQuery": self._selectQuery,
                "whereClause": self._whereClause,
                "partitionBy": self._copyIntoFilePartitionBy,
                "fileFormatName": self._fileFormatName,
            },
        )

        retCmd.append(SQLCommand(sqlCommand=cmd, sqlBinds=self.getBinds()))

        return retCmd
