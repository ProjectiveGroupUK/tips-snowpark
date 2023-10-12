from typing import List, Dict
from tips.framework.metadata.additional_field import AdditionalField


class ActionMetadata:
    _cmdType: str
    _source: str
    _target: str
    _whereClause: str
    _additionalFields: list
    _binds: list
    _createTempTable: bool
    _pivotField: str
    _pivotBy: str
    _businessKey: str
    _refreshType: str
    _mergeOnFields: list
    _generateMergeMatchedClause: bool
    _generateMergeWhenNotMatchedClause: bool
    _isActive: bool
    _cmdDQTests: list
    _fileFormatName: str
    _copyIntoFilePartitionBy: str
    _processCmdId: int
    _copyAutoMapping: str
    _copyIntoForce: str

    def __init__(
        self,
        cmdType: str,
        source: str,
        target: str,
        whereClause: str,
        additionalFields: List[AdditionalField],
        binds: List[Dict],
        createTempTable: bool,
        pivotField: str,
        pivotBy: str,
        businessKey: str,
        refreshType: str,
        mergeOnFields: List[str],
        generateMergeMatchedClause: bool,
        generateMergeWhenNotMatchedClause: bool,
        isActive: bool,
        cmdDQTests: List,
        fileFormatName: str,
        copyIntoFilePartitionBy: str,
        processCmdId: int,
        copyAutoMapping: str,
        copyIntoForce: str
    ) -> None:
        self._cmdType = cmdType
        self._source = source
        self._target = target
        self._whereClause = whereClause
        self._additionalFields = additionalFields
        self._binds = binds
        self._createTempTable = createTempTable
        self._pivotField = pivotField
        self._pivotBy = pivotBy
        self._businessKey = businessKey
        self._refreshType = refreshType
        self._mergeOnFields = mergeOnFields
        self._generateMergeMatchedClause = generateMergeMatchedClause
        self._generateMergeWhenNotMatchedClause = generateMergeWhenNotMatchedClause
        self._isActive = isActive
        self._cmdDQTests = cmdDQTests
        self._fileFormatName = fileFormatName
        self._copyIntoFilePartitionBy = copyIntoFilePartitionBy
        self._processCmdId = processCmdId
        self._copyAutoMapping = copyAutoMapping
        self._copyIntoForce = copyIntoForce

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

    def getPivotField(self) -> str:
        return self._pivotField

    def getPivotBy(self) -> str:
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

    def getFileFormatName(self) -> str:
        return self._fileFormatName

    def getCopyIntoFilePartitionBy(self) -> str:
        return self._copyIntoFilePartitionBy

    def getProcessCmdId(self) -> int:
        return self._processCmdId
    
    def getCopyAutoMapping(self) -> str:
        return self._copyAutoMapping
    
    def getCopyIntoForce(self) -> str:
        return self._copyIntoForce