from typing import List
from tips.framework.actions.sql_action import SqlAction
from tips.framework.actions.sql_command import SQLCommand
from tips.framework.utils.sql_template import SQLTemplate


class DeleteAction(SqlAction):
    _target: str
    _whereClause: str
    _binds: List[str]

    def __init__(self, target: str, whereClause: str, binds: List[str]) -> None:
        self._target = target
        self._whereClause = whereClause
        self._binds = binds

    def getBinds(self) -> List[str]:
        return self._binds

    def getCommands(self) -> List[object]:
        retCmd: List[object] = []

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

        cmd: str = SQLTemplate().getTemplate(
            sqlAction="delete",
            parameters={
                "target": self._target,
                "whereClause": self._whereClause,
            },
        )

        retCmd.append(SQLCommand(sqlCommand=cmd, sqlBinds=self.getBinds()))

        return retCmd
