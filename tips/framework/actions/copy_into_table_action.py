from typing import List

from tips.framework.actions.sql_action import SqlAction
from tips.framework.actions.sql_command import SQLCommand
from tips.framework.metadata.column_info import ColumnInfo
from tips.framework.metadata.table_metadata import TableMetaData
from tips.framework.utils.sql_template import SQLTemplate
from tips.framework.utils.globals import Globals


class CopyIntoTableAction(SqlAction):
    _source: str
    _target: str
    _binds: List[str]
    _fileFormatName: str

    def __init__(
        self,
        source: str,
        target: str,
        binds: List[str],
        fileFormatName: str,
    ) -> None:
        self._source = source
        self._target = target
        self._binds = binds
        self._fileFormatName = fileFormatName

    def getBinds(self) -> List[str]:
        return self._binds

    def getCommands(self) -> List[object]:
        retCmd: List[object] = []

        ## append quotes with bind variable
        # cnt = 0
        # while True:
        #     cnt += 1
        #     if self._whereClause is not None and f":{cnt}" in self._whereClause:
        #         self._whereClause = (
        #             self._whereClause.replace(f":{cnt}", f"':{cnt}'")
        #             if self._whereClause is not None
        #             else None
        #         )
        #     else:
        #         break

        
        cmd: str = SQLTemplate().getTemplate(
            sqlAction="copy_into_table",
            parameters={
                "fileName": self._source,
                "tableName": self._target,
                "fileFormatName": self._fileFormatName
            },
        )

        retCmd.append(SQLCommand(sqlCommand=cmd, sqlBinds=self.getBinds()))

        return retCmd
