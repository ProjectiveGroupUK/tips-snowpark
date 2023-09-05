class ColumnInfo:
    _columnName: str
    _datatype: str
    _isVirtual: bool
    _isPK: bool
    _sequenceName: str

    def __init__(
        self,
        columnName: str,
        datatype: str,
        isVirtual: bool,
        isPK: bool,
        sequenceName: str,
    ) -> None:
        self._columnName = columnName
        self._datatype = datatype
        self._isVirtual = isVirtual
        self._isPK = isPK
        self._sequenceName = sequenceName

    def getColumnName(self) -> str:
        return self._columnName

    def getDatatype(self) -> str:
        return self._datatype

    def setDatatype(self, datatype: str):
        self._datatype = datatype

    def isVirtual(self) -> bool:
        return self._isVirtual

    def isPK(self) -> bool:
        return self._isPK

    def getSequenceName(self) -> str:
        return self._sequenceName

    def __str__(self) -> str:
        return f"ColumnInfo [columnName={self._columnName}, datatype={self._datatype}, isPK={self._isPK}, isVirtual={self._isVirtual}, sequenceName={self._sequenceName}]"
