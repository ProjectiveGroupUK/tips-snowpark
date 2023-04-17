from typing import List
from tips.framework.actions.sql_action import SqlAction
from tips.framework.actions.sql_command import SQLCommand


class DQTestAction(SqlAction):
    _cmdDQTests: List

    def __init__(
        self,
        cmdDQTests: List,
        whereClause: str,
        binds: List[str],
    ) -> None:
        self._cmdDQTests = cmdDQTests
        self._whereClause = whereClause
        self._binds = binds

    def getBinds(self) -> List[str]:
        return self._binds

    def getCommands(self) -> List[object]:
        retCmd: List[object] = []

        if len(self._cmdDQTests) > 0:
            for cmdDQTest in self._cmdDQTests:

                dqQuery = cmdDQTest["PROCESS_DQ_TEST_QUERY_TEMPLATE"].strip()

                if "{COL_NAME}" in dqQuery:
                    dqQuery = dqQuery.replace("{COL_NAME}", cmdDQTest["ATTRIBUTE_NAME"])

                if "{TAB_NAME}" in dqQuery:
                    dqQuery = dqQuery.replace("{TAB_NAME}", cmdDQTest["TGT_NAME"])

                if "{ACCEPTED_VALUES}" in dqQuery:
                    dqQuery = dqQuery.replace(
                        "{ACCEPTED_VALUES}", cmdDQTest["ACCEPTED_VALUES"]
                    )

                if self._whereClause is not None and self._whereClause != "":
                    if 'where' in dqQuery.lower():
                        dqQuery = dqQuery + " AND " + self._whereClause
                    else:
                        dqQuery = dqQuery + " WHERE " + self._whereClause

                # cmd:str = f"SELECT COUNT(*) AS DQ_TEST_FAILED_COUNT FROM ({dqQuery})"
                cmd: str = dqQuery

                retCmd.append(
                    SQLCommand(
                        sqlCommand=cmd, sqlBinds=self.getBinds(), dqCheckDict=cmdDQTest
                    )
                )

        return retCmd
