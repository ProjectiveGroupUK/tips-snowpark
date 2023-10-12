from typing import List, Dict
from tips.framework.metadata.additional_field import AdditionalField
from tips.framework.metadata.column_info import ColumnInfo


class TableMetaData:
    _metadata: Dict[str, List[ColumnInfo]]

    def __init__(self, metadata: Dict[str, List[ColumnInfo]]):
        self._metadata = metadata

    def addMetaData(self, tableName: str, columnInfo: List[ColumnInfo]) -> None:
        self._metadata[tableName] = columnInfo

    def getColumns(
        self, tableName: str, excludeVirtualColumns: bool
    ) -> List[ColumnInfo]:
        cols: List[ColumnInfo] = list()


        if excludeVirtualColumns:
            ## TBC
            if self._metadata.get(tableName) is not None:
                for col in self._metadata.get(tableName):
                    if not col.isVirtual():
                        cols.append(col)
        else:
            cols = self._metadata.get(tableName)

        return cols

    def getCommonColumns(
        self, srcTableName: str, tgtTableName: str
    ) -> List[ColumnInfo]:
        commonColumns: List[ColumnInfo] = list()

        tgtColumns: List[ColumnInfo] = self.getColumns(tgtTableName, True)
        srcColumns: List[ColumnInfo] = self.getColumns(srcTableName, True)

        if len(tgtColumns) > 0:
            for col in tgtColumns:
                if any(
                    col.getColumnName() == srcCol.getColumnName()
                    for srcCol in srcColumns
                ):
                    commonColumns.append(col)

        return commonColumns

    def getPKColumns(self, tableName: str) -> List[ColumnInfo]:
        pk_cols: List[ColumnInfo] = []

        for col in self._metadata.get(tableName):
            if col.isPK():
                pk_cols.append(col)

        return pk_cols

    def getColumnsWithSequence(self, tableName: str) -> List[ColumnInfo]:
        seq_cols: List[ColumnInfo] = []

        for col in self._metadata.get(tableName):
            if col.getSequenceName() is not None or col.getSequenceName() != "null":
                seq_cols.append(col)

        return seq_cols

    def getCommaDelimited(self, cols: list) -> str:
        separator = ", "

        return separator.join(cols)

    def getSelectAndFieldClauses(
        self,
        commonColumns: List[ColumnInfo],
        additionalFields: List[AdditionalField],
    ) -> Dict[str, List[str]]:
        fieldLists: Dict[str, List[str]] = {}

        selectClause: List[str] = []
        fieldClause: List[str] = []

        for col in commonColumns:
            selectClause.append(col.getColumnName())
            fieldClause.append(col.getColumnName())

        for af in additionalFields:
            ## Only add the addtional column if an existing alias doesn't already exist.  Case sensitive at the moment
            if af.getAlias() not in fieldClause:
                selectClause.append(af.getField())
                fieldClause.append(af.getAlias())

        fieldLists["SelectClause"] = selectClause
        fieldLists["FieldClause"] = fieldClause

        return fieldLists
    
    def getDollarSelectOrdered(
        self,
        srcColumnNames: List[str],
        selectColumns: List[ColumnInfo],
    ) -> List[str]:

        #numbering select by source file order
        selectClause = ['$'+str(srcColumnNames.index(col.getColumnName()) + 1) for col in selectColumns]

        return selectClause
