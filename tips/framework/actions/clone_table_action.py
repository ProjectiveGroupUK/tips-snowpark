from typing import List

from tips.framework.actions.sql_action import SqlAction
from tips.framework.actions.sql_command import SQLCommand
from tips.framework.metadata.column_info import ColumnInfo
from tips.framework.metadata.table_metadata import TableMetaData
from tips.framework.utils.sql_template import SQLTemplate


class CloneTableAction(SqlAction):
    _source: str
    _target: str
    _tableMetaData: TableMetaData
    _isTempTable: bool

    def __init__(
        self,
        source: str,
        target: str,
        tableMetaData: TableMetaData,
        isTempTable: bool = False,
    ) -> None:
        self._source = source
        self._target = target
        self._tableMetaData = tableMetaData
        self._isTempTable = isTempTable
        self.cloneMetadata()

    def getBinds(self) -> List[str]:
        pass

    def getCommands(self) -> List[object]:
        retCmd: List[object] = []

        cmd: str = SQLTemplate().getTemplate(
            sqlAction="clone_table",
            parameters={
                "source": self._source,
                "target": self._target,
                "isTempTable": True
                if self._source == self._target
                else self._isTempTable,
            },
        )

        retCmd.append(SQLCommand(cmd))

        ## if it is normal clone command, drop any columns that are populated using sequences (key columns)
        if self._source != self._target:
            seqCols: List[ColumnInfo] = self._tableMetaData.getColumnsWithSequence(
                self._source
            )
            colList: List[str] = list()

            for seqCol in seqCols:
                if seqCol.getSequenceName() is not None:
                    colList.append(seqCol.getColumnName())

            if len(colList) > 0:
                separator = ", "
                commaDelimitedList = separator.join(colList)

                cmd = f"ALTER TABLE {self._target} DROP COLUMN {commaDelimitedList}"

                retCmd.append(SQLCommand(cmd))

        return retCmd

    def cloneMetadata(self) -> None:
        ## if it is normal clone command, drop any columns that are populated using sequences (key columns)
        if self._source != self._target:
            ## copy MetaData of source to target
            targetTableMetaData = self._tableMetaData.getColumns(
                tableName=self._source, excludeVirtualColumns=True
            )

            seqCols: List[ColumnInfo] = self._tableMetaData.getColumnsWithSequence(
                self._source
            )

            for seqCol in seqCols:
                if seqCol.getSequenceName() is not None:
                    targetTableMetaData.remove(seqCol)

            ## add table MetaData of target
            self._tableMetaData.addMetaData(self._target, targetTableMetaData)
