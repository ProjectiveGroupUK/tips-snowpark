from typing import List
from tips.framework.actions.sql_action import SqlAction


class DefaultAction(SqlAction):

    def getBinds(self) -> List[str]:
        pass

    def getCommands(self) -> List[object]:
        pass
