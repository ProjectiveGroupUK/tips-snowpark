from tips.framework.actions.append_action import AppendAction
from tips.framework.actions.sql_action import SqlAction
from tips.framework.metadata.additional_field import AdditionalField
from tips.framework.metadata.table_metadata import TableMetaData
from typing import List


class OIRefreshAction(SqlAction):
    _source: str
    _target: str
    _whereClause: str
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
        isCreateTempTable: bool,
    ) -> None:
        self._source = source
        self._target = target
        self._whereClause = whereClause
        self._metadata = metadata
        self._binds = binds
        self._additionalFields = additionalFields
        self._isCreateTempTable = isCreateTempTable

    def getBinds(self) -> List[str]:
        return self._binds

    def getCommands(self) -> List[object]:

        isOverwrite: bool = True

        cmd: List[object] = []

        cmd.append(
            AppendAction(
                source=self._source,
                target=self._target,
                whereClause=self._whereClause,
                metadata=self._metadata,
                binds=self._binds,
                additionalFields=self._additionalFields,
                isOverwrite=isOverwrite,
                isCreateTempTable=self._isCreateTempTable,
            )
        )

        return cmd
