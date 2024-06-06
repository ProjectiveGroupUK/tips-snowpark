from typing import Dict, List

from tips.framework.actions.sql_action import SqlAction
from tips.framework.actions.sql_command import SQLCommand
from tips.framework.metadata.additional_field import AdditionalField
from tips.framework.metadata.table_metadata import TableMetaData
from tips.framework.metadata.column_info import ColumnInfo
from tips.framework.utils.sql_template import SQLTemplate
from tips.framework.actions.clone_table_action import CloneTableAction
from tips.framework.utils.globals import Globals


class CallProcedureAction(SqlAction):
    _source: str
    _metadata: TableMetaData
    _binds: List[str]

    def __init__(
        self,
        source: str,
        binds: List[str],
    ) -> None:
        self._source = source
        self._binds = binds

    def getBinds(self) -> List[str]:
        return self._binds

    def getCommands(self) -> List[object]:
        globalsInstance = Globals()

        cmd: List[object] = []

        cmdStr = SQLTemplate().getTemplate(
            sqlAction="call_procedure",
            parameters={
                "source": self._source
            },
        )

        cmd.append(SQLCommand(sqlCommand=cmdStr, sqlBinds=self.getBinds()))

        return cmd
