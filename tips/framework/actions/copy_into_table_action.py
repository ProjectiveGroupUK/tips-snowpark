from typing import List

from tips.framework.actions.sql_action import SqlAction
from tips.framework.actions.sql_command import SQLCommand
from tips.framework.metadata.column_info import ColumnInfo
from tips.framework.metadata.table_metadata import TableMetaData
from tips.framework.utils.sql_template import SQLTemplate
from tips.framework.utils.globals import Globals
from tips.framework.metadata.additional_field import AdditionalField


class CopyIntoTableAction(SqlAction):
    _source: str
    _target: str
    _binds: List[str]
    _fileFormatName: str
    _additionalFields: List[AdditionalField]
    _copyAutoMapping: str
    _metadata: TableMetaData


    def __init__(
        self,
        source: str,
        target: str,
        binds: List[str],
        fileFormatName: str,
        additonalFields: List[AdditionalField],
        copyAutoMapping: str,
        metadata: TableMetaData
    ) -> None:
        self._source = source
        self._target = target
        self._binds = binds
        self._fileFormatName = fileFormatName
        self._additionalFields = additonalFields
        self._copyAutoMapping = copyAutoMapping
        self._metadata = metadata

    def getBinds(self) -> List[str]:
        return self._binds

    def getCommands(self) -> List[object]:
        retCmd: List[object] = []
        globalsInstance = Globals()
        session = globalsInstance.getSession()
        targetDatabase = globalsInstance.getTargetDatabase()

        ## append quotes with bind variable
        # cnt = 0
        # while True:
        #     cnt += 1
        #     if self._whereClause is not None and f":{cnt}" in self._whereClause:
        #         self._whereClause = (
        #             self._whereClause.replace(f":{cnt}", f"':{cnt}'")
        #             if self._whereClause is not None
        #             else None
        #         )
        #     else:
        #         break

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
        
        #auto field mapping or additinal fields requires select statement
        if len(self._additionalFields) != 0 or self._copyAutoMapping == 'Y':
            #target table field names
            tgtColumns: List[ColumnInfo] = self._metadata.getColumns(tableName=self._target, excludeVirtualColumns=False)

            #remove additional fields from this list, added later with correct expression
            addFieldAliases = [field.getAlias() for field in self._additionalFields]
            tgtColumns = [col for col in tgtColumns if col.getColumnName() not in addFieldAliases]
            
            #auto field mapping
            if self._copyAutoMapping == 'Y':
                #file_format parse_header required for infer_schema
                alterFileFormat = f'ALTER FILE FORMAT {self._fileFormatName}\
                                    SET PARSE_HEADER = TRUE SKIP_HEADER = 0'          
                session.sql(alterFileFormat).collect()

                #inferring fields in source
                inferQuery =   f'SELECT UPPER(COLUMN_NAME) AS COLUMN_NAME\
                                FROM TABLE(\
                                    INFER_SCHEMA(\
                                        LOCATION=>\'{sourceName}\',\
                                        FILE_FORMAT=>\'{self._fileFormatName}\'))\
                                ORDER BY ORDER_ID ASC;'
                inferQueryRes = session.sql(inferQuery).collect()

                #undo file_format alter
                dealterFileFormat = f'ALTER FILE FORMAT {self._fileFormatName}\
                                    SET PARSE_HEADER = FALSE SKIP_HEADER = 1'
                session.sql(dealterFileFormat).collect()

            
                srcColumnNames = [row.COLUMN_NAME for row in inferQueryRes]
            
                #fields in source and target
                commonFields = [col for col in tgtColumns if col.getColumnName() in srcColumnNames]
                
                selectList: List[str] = self._metadata.getDollarSelectOrdered(srcColumnNames, commonFields)
            
            else:
                #creating dollar select
                selectList = ['$'+str(i+1) for i in range(len(tgtColumns))]

            for af in self._additionalFields:
                ## Only add the addtional column if an existing alias doesn't already exist.  Case sensitive at the moment
                if af.getAlias() not in selectList:
                    selectList.append(af.getField())          
            
            selectClause = self._metadata.getCommaDelimited(selectList)
            

            ## append quotes with bind variable
            cnt = 0
            while True:
                cnt += 1
                if (
                    #(self._whereClause is not None and f":{cnt}" in self._whereClause)
                    selectClause is not None and f":{cnt}" in selectClause
                ):
                    # self._whereClause = (
                    #     self._whereClause.replace(f":{cnt}", f"':{cnt}'")
                    #     if self._whereClause is not None
                    #     else None
                    # )
                    selectClause = (
                        selectClause.replace(f":{cnt}", f"':{cnt}'")
                        if selectClause is not None
                        else None
                    )
                else:
                    break
            
            cmd: str = SQLTemplate().getTemplate(
                sqlAction="copy_into_table_from_select",
                parameters={
                    "fileName": sourceName,
                    "tableName": self._target,
                    "fileFormatName": self._fileFormatName,
                    "selectList": selectClause
                },
            )


        #standard COPY INTO
        else:
            cmd: str = SQLTemplate().getTemplate(
                sqlAction="copy_into_table",
                parameters={
                    "fileName": sourceName,
                    "tableName": self._target,
                    "fileFormatName": self._fileFormatName,
                },
            )


        retCmd.append(SQLCommand(sqlCommand=cmd, sqlBinds=self.getBinds()))

        return retCmd
            