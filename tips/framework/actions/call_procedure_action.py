from typing import Dict, List

from tips.framework.actions.sql_action import SqlAction
from tips.framework.actions.sql_command import SQLCommand
from tips.framework.metadata.table_metadata import TableMetaData
from tips.framework.utils.sql_template import SQLTemplate
from tips.framework.utils.globals import Globals


class CallProcedureAction(SqlAction):
    _source: str
    _whereClause: str
    _metadata: TableMetaData
    _binds: List[str]

    def __init__(
        self,
        source: str,
        whereClause: str,
        binds: List[str],
    ) -> None:
        self._source = source
        self._whereClause = whereClause
        self._binds = binds

    def getBinds(self) -> List[str]:
        return self._binds

    def getCommands(self) -> List[object]:
        globalsInstance = Globals()

        cmd: List[object] = []

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
        
        #use where clause in proc call, requires syntax change
        self._whereClause=self._whereClause.replace('=','=>')

        cmdStr = SQLTemplate().getTemplate(
            sqlAction="call_procedure",
            parameters={
                "source": self._source,
                "whereClause": self._whereClause
            },
        )

        cmd.append(SQLCommand(sqlCommand=cmdStr, sqlBinds=self.getBinds()))

        return cmd
