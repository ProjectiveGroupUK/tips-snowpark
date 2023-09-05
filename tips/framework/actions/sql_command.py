from typing import Dict, List


class SQLCommand:
    _sqlCommand: str
    _sqlBinds: List[str]
    _sqlChecks: List[Dict]
    _dqCheckDict: Dict

    def __init__(
        self,
        sqlCommand: str,
        sqlBinds: List[str] = None,
        sqlChecks: List[Dict] = None,
        dqCheckDict: Dict = None,
    ) -> None:
        self._sqlCommand = sqlCommand
        self._sqlBinds = sqlBinds
        self._sqlChecks = sqlChecks
        self._dqCheckDict = dqCheckDict

    def getSqlCommand(self) -> str:
        return self._sqlCommand

    def getSqlBinds(self) -> List[str]:
        return self._sqlBinds

    def getSqlChecks(self) -> List[Dict]:
        return self._sqlChecks

    def getDQCheckDict(self) -> Dict:
        return self._dqCheckDict
