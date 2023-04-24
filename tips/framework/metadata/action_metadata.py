from typing import List, Dict
from tips.framework.metadata.additional_field import AdditionalField


class ActionMetadata():
    _cmdType: str
    _source: str
    _target: str
    _whereClause: str
    _additionalFields: list
    _binds: list
    _createTempTable: bool
    _pivotBy: list
    _businessKey: str
    _refreshType: str
    _mergeOnFields: list
    _generateMergeMatchedClause: bool
    _generateMergeWhenNotMatchedClause: bool
    _isActive: bool
    _cmdDQTests: list

    def __init__(self, cmdType: str, source: str, target: str,
                 whereClause: str, additionalFields: List[AdditionalField],
                 binds: List[Dict], createTempTable: bool, pivotBy: List[str],
                 businessKey: str, refreshType: str, mergeOnFields: List[str],
                 generateMergeMatchedClause: bool,
                 generateMergeWhenNotMatchedClause: bool,
                 isActive: bool,
                 cmdDQTests: List) -> None:

        self._cmdType = cmdType
        self._source = source
        self._target = target
        self._whereClause = whereClause
        self._additionalFields = additionalFields
        self._binds = binds
        self._createTempTable = createTempTable
        self._pivotBy = pivotBy
        self._businessKey = businessKey
        self._refreshType = refreshType
        self._mergeOnFields = mergeOnFields
        self._generateMergeMatchedClause = generateMergeMatchedClause
        self._generateMergeWhenNotMatchedClause = generateMergeWhenNotMatchedClause
        self._isActive = isActive
        self._cmdDQTests = cmdDQTests

    def getCmdType(self) -> str:
        return self._cmdType

    def getSource(self) -> str:
        return self._source

    def getTarget(self) -> str:
        return self._target

    def getWhereClause(self) -> str:
        return self._whereClause

    def getAdditionalFields(self) -> List[AdditionalField]:
        return self._additionalFields

    def getBinds(self) -> List[Dict]:
        return self._binds

    def isCreateTempTable(self) -> bool:
        return self._createTempTable

    def getPivotBy(self) -> List[str]:
        return self._pivotBy

    def getBusinessKey(self) -> str:
        return self._businessKey

    def getRefreshType(self) -> str:
        return self._refreshType

    def getMergeOnFields(self) -> List[str]:
        return self._mergeOnFields

    def isGenerateMergeMatchedClause(self) -> bool:
        return self._generateMergeMatchedClause

    def isGenerateMergeWhenNotMatchedClause(self) -> bool:
        return self._generateMergeWhenNotMatchedClause

    def isActive(self) -> bool:
        return self._isActive

    def getCmdDQTests(self) -> List:
        return self._cmdDQTests
