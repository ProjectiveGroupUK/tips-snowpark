from typing import List
from tips.framework.actions.sql_action import SqlAction
from tips.framework.actions.sql_command import SQLCommand
from tips.framework.utils.sql_template import SQLTemplate


class TruncateAction(SqlAction):
    _target: str

    def __init__(self, target: str) -> None:
        self._target = target

    def getBinds(self) -> List[str]:
        return self._binds

    def getCommands(self) -> List[object]:
        retCmd: List[object] = []

        cmd: str = SQLTemplate().getTemplate(
            sqlAction="truncate", parameters={"target": self._target}
        )

        retCmd.append(SQLCommand(sqlCommand=cmd))

        return retCmd
