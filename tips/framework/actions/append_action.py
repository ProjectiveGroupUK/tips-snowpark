from typing import Dict, List

from tips.framework.actions.sql_action import SqlAction
from tips.framework.actions.sql_command import SQLCommand
from tips.framework.metadata.additional_field import AdditionalField
from tips.framework.metadata.table_metadata import TableMetaData
from tips.framework.metadata.column_info import ColumnInfo
from tips.framework.utils.sql_template import SQLTemplate


class AppendAction(SqlAction):
    _source: str
    _target: str
    _whereClause: str
    _isOverwrite: bool
    _metadata: TableMetaData
    _binds: List[str]
    _additionalFields: List[AdditionalField]
    _isCreateTempTable: bool

    def __init__(
        self,
        source: str,
        target: str,
        whereClause: str,
        metadata: TableMetaData,
        binds: List[str],
        additionalFields: List[AdditionalField],
        isOverwrite: bool,
        isCreateTempTable: bool,
    ) -> None:
        self._source = source
        self._target = target
        self._whereClause = whereClause
        self._isOverwrite = isOverwrite
        self._metadata = metadata
        self._binds = binds
        self._additionalFields = additionalFields
        self._isCreateTempTable = isCreateTempTable

    def getBinds(self) -> List[str]:
        return self._binds

    def getCommands(self) -> List[object]:

        cmd: List[object] = []

        ## if temp table flag is set on metadata, than create a temp table with same name as target
        ## in same schema
        if self._isCreateTempTable:
            cmdStr: str = SQLTemplate().getTemplate(
                sqlAction="clone_table",
                parameters={"source": self._target, "target": self._target, "isTempTable": True},
            )

            cmd.append(SQLCommand(cmdStr))
        commonColumns: List[ColumnInfo] = self._metadata.getCommonColumns(
            self._source, self._target
        )

        fieldLists: Dict[str, List[str]] = self._metadata.getSelectAndFieldClauses(
            commonColumns, self._additionalFields
        )

        selectClause: List[str] = fieldLists.get("SelectClause")
        fieldClause: List[str] = fieldLists.get("FieldClause")

        selectList = self._metadata.getCommaDelimited(selectClause)
        fieldList = self._metadata.getCommaDelimited(fieldClause)

        ## append quotes with bind variable
        cnt = 0
        while True:
            cnt += 1
            if (self._whereClause is not None and f':{cnt}' in self._whereClause) or (selectList is not None and f':{cnt}' in selectList) or (fieldList is not None and f':{cnt}' in fieldList):
                self._whereClause = self._whereClause.replace(f':{cnt}', f"':{cnt}'") if self._whereClause is not None else None
                selectList = selectList.replace(f':{cnt}', f"':{cnt}'") if selectList is not None else None
                fieldList = fieldList.replace(f':{cnt}', f"':{cnt}'") if fieldList is not None else None       
            else:
                break

        cmdStr = SQLTemplate().getTemplate(
            sqlAction="insert",
            parameters={
                "isOverwrite": self._isOverwrite,
                "target": self._target,
                "fieldList": fieldList,
                "selectList": selectList,
                "source": self._source,
                "whereClause": self._whereClause,
            },
        )

        cmd.append(SQLCommand(sqlCommand=cmdStr, sqlBinds=self.getBinds()))

        return cmd
