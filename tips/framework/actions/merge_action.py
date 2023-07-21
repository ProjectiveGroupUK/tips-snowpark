from typing import Dict, List
from tips.framework.actions.clone_table_action import CloneTableAction

from tips.framework.actions.sql_action import SqlAction
from tips.framework.actions.sql_command import SQLCommand
from tips.framework.metadata.additional_field import AdditionalField
from tips.framework.metadata.table_metadata import TableMetaData
from tips.framework.metadata.column_info import ColumnInfo
from tips.framework.utils.sql_template import SQLTemplate
from tips.framework.utils.globals import Globals


class MergeAction(SqlAction):
    _source: str
    _target: str
    _whereClause: str
    _metadata: TableMetaData
    _binds: List[str]
    _additionalFields: List[AdditionalField]
    _mergeOnFields: list
    _generateMergeMatchedClause: bool
    _generateMergeWhenNotMatchedClause: bool
    _isCreateTempTable: bool

    def __init__(
        self,
        source: str,
        target: str,
        whereClause: str,
        metadata: TableMetaData,
        binds: List[str],
        additionalFields: List[AdditionalField],
        mergeOnFields: list,
        generateMergeMatchedClause: bool,
        generateMergeWhenNotMatchedClause: bool,
        isCreateTempTable: bool,
    ) -> None:
        self._source = source
        self._target = target
        self._whereClause = whereClause
        self._metadata = metadata
        self._binds = binds
        self._additionalFields = additionalFields
        self._mergeOnFields = mergeOnFields
        self._generateMergeMatchedClause = generateMergeMatchedClause
        self._generateMergeWhenNotMatchedClause = generateMergeWhenNotMatchedClause
        self._isCreateTempTable = isCreateTempTable

    def getBinds(self) -> List[str]:
        return self._binds

    def getCommands(self) -> List[object]:
        globalsInstance = Globals()
        cmd: List[object] = []
        insertFieldList: str = None
        updateFieldList: str = None
        valueFieldList: str = None

        ## if temp table flag is set on metadata, than create a temp table with same name as target
        ## in same schema
        if (
            self._isCreateTempTable
            and globalsInstance.isNotCalledFromNativeApp()  ## Native apps don't allow create table or create temporary table privilege
        ):
            cmd.append(
                CloneTableAction(
                    source=self._target,
                    target=self._target,
                    tableMetaData=self._metadata,
                    isTempTable=True,
                )
            )

        commonColumns: List[ColumnInfo] = self._metadata.getCommonColumns(
            self._source, self._target
        )

        fieldLists: Dict[str, List[str]] = self._metadata.getSelectAndFieldClauses(
            commonColumns, self._additionalFields
        )

        selectClause: List[str] = fieldLists.get("SelectClause")
        fieldClause: List[str] = fieldLists.get("FieldClause")

        selectList = self._metadata.getCommaDelimited(selectClause)

        mergeList: List = list()
        for fld in self._mergeOnFields:
            splittedField = fld.strip().lower()
            if splittedField != "":
                mergeList.append(f"s.{splittedField} = t.{splittedField}")

        if len(mergeList) > 0:
            mergeOnFieldList = " AND ".join(mergeList)

        ## Generate update column list
        if self._generateMergeMatchedClause:
            updateList: List = list()
            for field in fieldClause:
                if field not in self._mergeOnFields:
                    updateList.append(f"t.{field} = s.{field}")

            if len(updateList) > 0:
                updateFieldList = ", ".join(updateList)

        ## Generate insert column list
        if self._generateMergeWhenNotMatchedClause:
            tgtTableColumns = self._metadata.getColumns(
                tableName=self._target, excludeVirtualColumns=True
            )
            insertList: List = list()
            valueList: List = list()
            for field in tgtTableColumns:
                if field.getColumnName() in fieldClause:
                    insertList.append(field.getColumnName())
                    valueList.append(f"s.{field.getColumnName()}")
                elif field.getColumnName().endswith(
                    "_KEY"
                ) or field.getColumnName().endswith("_SEQ"):
                    if (
                        field.getSequenceName() is not None
                        and field.getSequenceName() != ""
                    ):
                        insertList.append(field.getColumnName())
                        valueList.append(f"{field.getSequenceName()}.nextval")

            if len(insertList) > 0:
                insertFieldList = ", ".join(insertList)
                valueFieldList = ", ".join(valueList)

        ## append quotes with bind variable
        cnt = 0
        while True:
            cnt += 1
            if (self._whereClause is not None and f":{cnt}" in self._whereClause) or (
                selectList is not None and f":{cnt}" in selectList
            ):
                self._whereClause = (
                    self._whereClause.replace(f":{cnt}", f"':{cnt}'")
                    if self._whereClause is not None
                    else None
                )
                selectList = (
                    selectList.replace(f":{cnt}", f"':{cnt}'")
                    if selectList is not None
                    else None
                )
            else:
                break

        cmdStr = SQLTemplate().getTemplate(
            sqlAction="merge",
            parameters={
                "target": self._target,
                "selectList": selectList,
                "source": self._source,
                "whereClause": self._whereClause,
                "mergeOnFieldList": mergeOnFieldList,
                "updateFieldList": updateFieldList,
                "insertFieldList": insertFieldList,
                "valueFieldList": valueFieldList,
            },
        )

        cmd.append(SQLCommand(sqlCommand=cmdStr, sqlBinds=self.getBinds()))

        return cmd
