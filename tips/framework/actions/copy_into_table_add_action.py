from typing import Dict, List

from tips.framework.actions.sql_action import SqlAction
from tips.framework.actions.sql_command import SQLCommand
from tips.framework.metadata.additional_field import AdditionalField
from tips.framework.metadata.table_metadata import TableMetaData
from tips.framework.metadata.column_info import ColumnInfo
from tips.framework.utils.sql_template import SQLTemplate
from tips.framework.utils.globals import Globals


class CopyIntoTableAddAction(SqlAction):
    _source: str
    _target: str
    _whereClause: str
    _isOverwrite: bool
    _metadata: TableMetaData
    _binds: List[str]
    _additionalFields: List[AdditionalField]
    _isCreateTempTable: bool
    _fileFormatName: str

    def __init__(
        self,
        source: str,
        target: str,
        #whereClause: str,
        metadata: TableMetaData,
        binds: List[str],
        additionalFields: List[AdditionalField],
        #isOverwrite: bool,
        #isCreateTempTable: bool,
        fileFormatName: str
    ) -> None:
        self._source = source
        self._target = target
        #self._whereClause = whereClause
        #self._isOverwrite = isOverwrite
        self._metadata = metadata
        self._binds = binds
        self._additionalFields = additionalFields
        #self._isCreateTempTable = isCreateTempTable
        self._fileFormatName = fileFormatName

    def getBinds(self) -> List[str]:
        return self._binds

    def getCommands(self) -> List[object]:
        retCmd: List[object] = []
        globalsInstance = Globals()
        session = globalsInstance.getSession()
        targetDatabase = globalsInstance.getTargetDatabase()

        cmd: List[object] = []

        ##Transpose stage name in filename with database and schema namespace
        if self._source.startswith("@"):
            slashPosition = self._source.find("/")
            if slashPosition == -1:  ##No slash(folder path exists)
                stageName = self._source[1:]
            else:
                stageName = self._source[1:slashPosition]

            ##Now check if there are any dots already. If there are 2 then we don't need to do anything
            if stageName.count(".") == 2:
                sourceName = self._source
            elif stageName.count(".") == 1:
                if slashPosition == -1:
                    sourceName = f"@{targetDatabase}.{stageName}"
                else:
                    sourceName = (
                        f"@{targetDatabase}.{stageName}{self._source[slashPosition:]}"
                    )
            elif stageName.count(".") == 0:
                currentSchema = session.sql(
                    "SELECT CURRENT_SCHEMA() AS CURR_SCHEMA"
                ).collect()[0]["CURR_SCHEMA"]
                if slashPosition == -1:
                    sourceName = f"@{targetDatabase}.{currentSchema}.{stageName}"
                else:
                    sourceName = f"@{targetDatabase}.{currentSchema}.{stageName}{self._source[slashPosition:]}"
            else:
                sourceName = self._source
        else:
            sourceName = self._source


        #target table field names
        tgtColumns: List[ColumnInfo] = self._metadata.getColumns(tableName=self._target, excludeVirtualColumns=False)


        #remove additional fields from this list, added later with correct expression
        addFieldAliases = [field.getAlias() for field in self._additionalFields]
        tgtColumns = [col for col in tgtColumns if col.getColumnName() not in addFieldAliases]

        selectClause: List[str] = self._metadata.getDollarSelectClause(tgtColumns,self._additionalFields)

        selectList = self._metadata.getCommaDelimited(selectClause)
        

        ## append quotes with bind variable
        cnt = 0
        while True:
            cnt += 1
            if (
                #(self._whereClause is not None and f":{cnt}" in self._whereClause)
                selectList is not None and f":{cnt}" in selectList
            ):
                # self._whereClause = (
                #     self._whereClause.replace(f":{cnt}", f"':{cnt}'")
                #     if self._whereClause is not None
                #     else None
                # )
                selectList = (
                    selectList.replace(f":{cnt}", f"':{cnt}'")
                    if selectList is not None
                    else None
                )
            else:
                break



        cmdStr = SQLTemplate().getTemplate(
            sqlAction="copy_into_table_add",
            parameters={
                "target": self._target,
                "selectList": selectList,
                "fileName": self._source,
                "fileFormatName" : self._fileFormatName,
            },
        )

        retCmd.append(SQLCommand(sqlCommand=cmdStr, sqlBinds=self.getBinds()))

        return retCmd
