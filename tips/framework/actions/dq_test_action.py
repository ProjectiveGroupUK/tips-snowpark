import re
from typing import List
from tips.framework.actions.sql_action import SqlAction
from tips.framework.actions.sql_command import SQLCommand
from snowflake.snowpark.row import Row as snowparkRow


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
            for val in self._cmdDQTests:
                if type(val) == snowparkRow:
                    cmdDQTest = val.as_dict(True)
                else:
                    cmdDQTest = val

                dqQuery = cmdDQTest["PROCESS_DQ_TEST_QUERY_TEMPLATE"].strip().upper()
                dqQuery = re.sub(' +', ' ', dqQuery) ##remove any double spaces

                if "{COL_NAME}" in dqQuery:
                    dqQuery = dqQuery.replace("{COL_NAME}", cmdDQTest["ATTRIBUTE_NAME"])

                if "{TAB_NAME}" in dqQuery:
                    dqQuery = dqQuery.replace("{TAB_NAME}", cmdDQTest["TGT_NAME"])

                if "{ACCEPTED_VALUES}" in dqQuery:
                    dqQuery = dqQuery.replace(
                        "{ACCEPTED_VALUES}", cmdDQTest["ACCEPTED_VALUES"]
                    )

                dqError = cmdDQTest["PROCESS_DQ_TEST_ERROR_MESSAGE"].strip()
                dqError = re.sub(' +', ' ', dqError) ##remove any double spaces

                if "{COL_NAME}" in dqError:
                    dqError = dqError.replace("{COL_NAME}", cmdDQTest["ATTRIBUTE_NAME"])

                if "{TAB_NAME}" in dqError:
                    dqError = dqError.replace("{TAB_NAME}", cmdDQTest["TGT_NAME"])

                if "{ACCEPTED_VALUES}" in dqError:
                    dqError = dqError.replace(
                        "{ACCEPTED_VALUES}", cmdDQTest["ACCEPTED_VALUES"]
                    )
                dqError = dqError.replace("'",'') ## remove any single quotes in error message
                cmdDQTest["DQ_ERROR_MESSAGE"] = dqError

                if self._whereClause is not None and self._whereClause != "":
                    currentStr:str = None
                    newStr:str = None
                    fromIdx = dqQuery.rfind("FROM")
                    whereIdx = dqQuery.rfind("WHERE")
                    groupByIdx = dqQuery.rfind("GROUP BY")
                    qualifyIdx = dqQuery.rfind("QUALIFY")
                    orderByIdx = dqQuery.rfind("ORDER BY")
                    limitIdx = dqQuery.rfind("LIMIT")

                    if (whereIdx == -1) or (whereIdx < fromIdx): # WHERE clause is either not present at all, or in last query
                        #Get string between FROM and GROUP BY, if GROUP BY is present
                        if (groupByIdx != -1) and (groupByIdx > fromIdx): ##Group By clause is present
                            currentStr = dqQuery[fromIdx:groupByIdx].strip()
                        elif (qualifyIdx != -1) and (qualifyIdx > fromIdx): ##QUALIFY clause is present
                            currentStr = dqQuery[fromIdx:qualifyIdx].strip()
                        elif (orderByIdx != -1) and (orderByIdx > fromIdx): ##ORDER BY clause is present
                            currentStr = dqQuery[fromIdx:orderByIdx].strip()
                        elif (limitIdx != -1) and (limitIdx > fromIdx): ##LIMIT clause is present
                            currentStr = dqQuery[fromIdx:limitIdx].strip()
                        else: #No clauses are present after FROM, so we take whole string post FROM
                            currentStr = dqQuery[fromIdx:].strip()

                        newStr = f"{currentStr} WHERE {self._whereClause}" 
                    else: # WHERE clause is present in last part of query
                        if (groupByIdx != -1) and (groupByIdx > whereIdx): ##Group By clause is present
                            currentStr = dqQuery[whereIdx:groupByIdx].strip()
                        elif (qualifyIdx != -1) and (qualifyIdx > whereIdx): ##QUALIFY clause is present
                            currentStr = dqQuery[whereIdx:qualifyIdx].strip()
                        elif (orderByIdx != -1) and (orderByIdx > whereIdx): ##ORDER BY clause is present
                            currentStr = dqQuery[whereIdx:orderByIdx].strip()
                        elif (limitIdx != -1) and (limitIdx > whereIdx): ##LIMIT clause is present
                            currentStr = dqQuery[whereIdx:limitIdx].strip()
                        else: #No clauses are present after WHERE, so we take whole string post WHERE
                            currentStr = dqQuery[whereIdx:].strip()
                        newStr = f"{currentStr} AND {self._whereClause}" 

                    if currentStr is not None and newStr is not None:
                        dqQuery = dqQuery.replace(currentStr,newStr)

                # cmd:str = f"SELECT COUNT(*) AS DQ_TEST_FAILED_COUNT FROM ({dqQuery})"
                cmd: str = dqQuery

                retCmd.append(
                    SQLCommand(
                        sqlCommand=cmd, sqlBinds=self.getBinds(), dqCheckDict=cmdDQTest
                    )
                )

        return retCmd
