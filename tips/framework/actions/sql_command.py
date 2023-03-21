from typing import Dict, List


class SQLCommand():
    _sqlCommand: str
    _sqlBinds: List[str]
    _sqlChecks: List[Dict]

    def __init__(self, sqlCommand: str, sqlBinds: List[str]=None, sqlChecks: List[Dict]=None) -> None:
        self._sqlCommand = sqlCommand
        self._sqlBinds = sqlBinds
        self._sqlChecks = sqlChecks

    def getSqlCommand(self) -> str:
        return self._sqlCommand

    def getSqlBinds(self) -> List[str]:
        return self._sqlBinds

    def getSqlChecks(self) -> List[Dict]:
        return self._sqlChecks        