from typing import List

from tips.framework.actions.sql_action import SqlAction
from tips.framework.actions.sql_command import SQLCommand
from tips.framework.metadata.column_info import ColumnInfo
from tips.framework.metadata.table_metadata import TableMetaData
from tips.framework.utils.sql_template import SQLTemplate
from tips.framework.utils.globals import Globals
from tips.framework.metadata.additional_field import AdditionalField


# Below is to initialise logging
import logging
from tips.utils.logger import Logger

logger = logging.getLogger(Logger.getRootLoggerName())

class CopyIntoTableAction(SqlAction):
    _source: str
    _target: str
    _binds: List[str]
    _fileFormatName: str
    _additionalFields: List[AdditionalField]
    _copyAutoMapping: str
    _metadata: TableMetaData
    _copyIntoForce: str


    def __init__(
        self,
        source: str,
        target: str,
        binds: List[str],
        fileFormatName: str,
        additonalFields: List[AdditionalField],
        copyAutoMapping: str,
        metadata: TableMetaData,
        copyIntoForce: str
    ) -> None:
        self._source = source
        self._target = target
        self._binds = binds
        self._fileFormatName = fileFormatName
        self._additionalFields = additonalFields
        self._copyAutoMapping = copyAutoMapping
        self._metadata = metadata
        self._copyIntoForce = copyIntoForce

    def getBinds(self) -> List[str]:
        return self._binds

    def getCommands(self) -> List[object]:
        retCmd: List[object] = []
        globalsInstance = Globals()
        session = globalsInstance.getSession()
        targetDatabase = globalsInstance.getTargetDatabase()

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
                
                #test if file is empty -> infer_schema not valid on empty files
                #temporary file format without parse_header


                #empty CSV will give a single row including header
                csvSizeTest = f'SELECT COUNT(*) AS ROW_COUNT FROM {sourceName}'
                csvRows = session.sql(csvSizeTest).collect()[0]['ROW_COUNT']

                if csvRows > 1:
                    emptyCsv = False
                else:
                    emptyCsv = True
                
                #if csv is not empty continue
                if not emptyCsv:

                    #inferring fields in source - requires PARSE_HEADER = TRUE, SKIP_HEADER=0
                    currentSessionId = session.sql('SELECT CURRENT_SESSION() AS SESSION_ID').collect()[0]['SESSION_ID']
                    tempFileFormat = f'{targetDatabase}.TIPS_MD_SCHEMA.format_{currentSessionId}'
                    #either use clone of given file format or create
                    if self._fileFormatName != None:
                        createTempFormat = f'CREATE OR REPLACE FILE FORMAT {tempFileFormat} CLONE {self._fileFormatName}'
                        session.sql(createTempFormat).collect()
                        alterTempFormat = f'ALTER FILE FORMAT {tempFileFormat} SET SKIP_HEADER = 0, PARSE_HEADER = TRUE'
                        session.sql(alterTempFormat).collect()
                    else:
                        createTempFormat = f'CREATE OR REPLACE FILE FORMAT {tempFileFormat} SKIP_HEADER = 0 PARSE_HEADER = TRUE'
                        session.sql(alterTempFormat).collect()
                    
                    inferQuery =   f'SELECT UPPER(COLUMN_NAME) AS COLUMN_NAME\
                                    FROM TABLE(\
                                        INFER_SCHEMA(\
                                            LOCATION=>\'{sourceName}\',\
                                            FILE_FORMAT=>\'{tempFileFormat}\'))\
                                    ORDER BY ORDER_ID ASC;'
                    inferQueryRes = session.sql(inferQuery).collect()

                    #drop temp file format
                    dropTempFormat = f'DROP FILE FORMAT {tempFileFormat}'
                    session.sql(dropTempFormat).collect()
                
                    srcColumnNames = [row.COLUMN_NAME for row in inferQueryRes]
            
                    #fields in source and target
                    commonFields = [col for col in tgtColumns if col.getColumnName() in srcColumnNames]
                    
                    selectList: List[str] = self._metadata.getDollarSelectOrdered(srcColumnNames, commonFields)
                
                #empty csv: no copy into command
                else:
                    return None
                


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
                    selectClause is not None and f":{cnt}" in selectClause
                ):
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
                    "selectList": selectClause,
                    "copyIntoForce": self._copyIntoForce
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
                    "copyIntoForce": self._copyIntoForce
                },
            )

        retCmd.append(SQLCommand(sqlCommand=cmd, sqlBinds=self.getBinds()))

        return retCmd
            