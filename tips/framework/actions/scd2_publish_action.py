from typing import Dict, List
from tips.framework.actions.append_action import AppendAction
from tips.framework.actions.clone_table_action import CloneTableAction
from tips.framework.actions.sql_action import SqlAction
from tips.framework.actions.sql_command import SQLCommand
from tips.framework.actions.truncate_action import TruncateAction
from tips.framework.metadata.additional_field import AdditionalField
from tips.framework.metadata.column_info import ColumnInfo
from tips.framework.metadata.table_metadata import TableMetaData
from tips.framework.utils.sql_template import SQLTemplate
import re


class SCD2PublishAction(SqlAction):
    _source: str
    _target: str
    _whereClause: str
    _businessKey: str
    _metadata: TableMetaData
    _binds: List[str]
    _additionalFields: List[AdditionalField]
    _isCreateTempTable: bool

    def __init__(
        self,
        source: str,
        target: str,
        whereClause: str,
        businessKey: str,
        metadata: TableMetaData,
        binds: List[str],
        additionalFields: List[AdditionalField],
        isCreateTempTable: bool,
    ) -> None:
        self._source = source
        self._target = target
        self._whereClause = whereClause
        self._businessKey = businessKey
        self._metadata = metadata
        self._binds = binds
        self._additionalFields = additionalFields
        self._isCreateTempTable = isCreateTempTable

    def getBinds(self) -> List[str]:
        return self._binds

    def getCommands(self) -> List[object]:

        cmd: List[object] = []
        cmdStr: str = str()
        interimTableName: str = str()

        ## if temp table flag is set on metadata, than create a temp table with same name as target
        ## in same schema
        if self._isCreateTempTable:
            cmd.append(
                CloneTableAction(
                    source=self._target,
                    target=self._target,
                    tableMetaData=self._metadata,
                    isTempTable=True
                )
            )

        ## Now clone the target table to an interim table
        interimTableName = f"{self._target.split('.')[0].strip()}.SRC_{self._target.split('.')[1].strip()}"
        cmd.append(
            CloneTableAction(
                source=self._target,
                target=interimTableName,
                tableMetaData=self._metadata,
                isTempTable=True
            )
        )

        # Populate intermin table
        ##Add IS_CURRENT_ROW and EFFECTIVE_END_DATE as additional fields, if these are not already part of view
        commonColumns = [col.getColumnName().upper() for col in self._metadata.getCommonColumns(self._source, self._target)]
        if "IS_CURRENT_ROW" not in commonColumns:
            self._additionalFields.append(AdditionalField(expression="TRUE",alias="IS_CURRENT_ROW"))

        if "EFFECTIVE_END_DATE" not in commonColumns:
            self._additionalFields.append(AdditionalField(expression="TO_DATE('99991231','YYYYMMDD')",alias="EFFECTIVE_END_DATE"))

        cmd.append(
            AppendAction(
                source=self._source,
                target=interimTableName,
                whereClause=self._whereClause,
                metadata=self._metadata,
                binds=self._binds,
                additionalFields=self._additionalFields,
                isOverwrite=False,
                isCreateTempTable=False,
            )
        )

        ## check that the COB hasn't already been loaded into the dimension
        selectFieldClause = "count(*) AS cnt"
        whereClause = f"effective_start_date > (SELECT MAX(effective_start_date) FROM {interimTableName})"
        sqlChecks: List = list()
        sqlCheck: Dict = dict()
        sqlCheck["condition"] = "['CNT'] != 0"
        sqlCheck["error"] = f"EFFECTIVE_START_DATE in {self._source} cannot be prior to existing records in {self._target}"
        sqlChecks.append(sqlCheck)

        cmdStr = SQLTemplate().getTemplate(
            sqlAction="select",
            parameters={
                "selectFieldClause": selectFieldClause,
                "source": self._target.lower(),
                "whereClause": whereClause,
            },
        )
        cmd.append(
            SQLCommand(sqlCommand=cmdStr, sqlBinds=self._binds, sqlChecks=sqlChecks)
        )

        ## check that all record in source are of same effective date
        selectFieldClause = (
            "count(distinct effective_start_date ) count_effective_start_date"
        )
        sqlChecks: List = list()
        sqlCheck: Dict = dict()
        sqlCheck["condition"] = "['COUNT_EFFECTIVE_START_DATE'] > 1"
        sqlCheck[
            "error"
        ] = f"{self._source} should only contain records with one EFFECTIVE_START_DATE"
        sqlChecks.append(sqlCheck)

        cmdStr = SQLTemplate().getTemplate(
            sqlAction="select",
            parameters={
                "selectFieldClause": selectFieldClause,
                "source": interimTableName,
            },
        )
        cmd.append(
            SQLCommand(sqlCommand=cmdStr, sqlBinds=self._binds, sqlChecks=sqlChecks)
        )

        ## Now generate MERGE statement
        commonColumns: List[ColumnInfo] = self._metadata.getCommonColumns(
            interimTableName, self._target
        )

        commonColumnsList: List = [
            col.getColumnName().lower().strip() for col in commonColumns
        ]

        # for col in commonColumns:
        srcColumnList = ", ".join("src." + col for col in commonColumnsList)

        dimColumnList = ", ".join(
            "0 as is_current_row"
            if col == "is_current_row"
            else "src.effective_start_date - 1 as effective_end_date"
            if col == "effective_end_date"
            else "dim." + col
            for col in commonColumnsList
        )

        seqCols: List[ColumnInfo] = self._metadata.getColumnsWithSequence(self._target)
        seqName: str = str()
        seqColName: str = str()

        for seqCol in seqCols:
            if seqCol.getSequenceName() is not None:
                seqName = seqCol.getSequenceName().lower().strip()
                seqColName = seqCol.getColumnName().lower().strip()


        businessKeyList: List = [
            businessKey.lower().strip() for businessKey in self._businessKey.split("|")
        ]

        businessKeyStr = " AND ".join(
            f"src.{businessKey} = dim.{businessKey}" for businessKey in businessKeyList
        )

        if businessKeyStr is None or businessKeyStr == "":
            businessKeyStr = "1 = 1"

        updateColList = list(
            filter(lambda ret: (ret not in businessKeyList), commonColumnsList)
        )

        updateColListStr = ", ".join(f"t.{col} = s.{col}" for col in updateColList)

        colList = ", ".join(col for col in commonColumnsList)

        sColList = ", ".join(f"s.{col}" for col in commonColumnsList)

        cmdStr = f"""
        MERGE INTO {self._target.lower()} t
        USING (
            WITH src AS
            (
                SELECT CASE WHEN dim.{seqColName} IS NOT NULL and src.effective_start_date = dim.effective_start_date THEN dim.{seqColName} ELSE NULL END AS {seqColName}
                , {srcColumnList}
                FROM {interimTableName.lower()} src
                LEFT JOIN {self._target.lower()} dim
                ON (dim.is_current_row = 1
                AND {businessKeyStr})
                WHERE src.binary_check_sum <> dim.binary_check_sum 
                OR dim.{seqColName} IS NULL
            )
            SELECT src.{seqColName}, {srcColumnList}
            FROM src
            UNION ALL
            SELECT dim.{seqColName}, {dimColumnList}
            FROM {self._target.lower()} dim
            JOIN src 
            ON ({businessKeyStr})
            WHERE dim.is_current_row = 1 
            AND src.{seqColName} IS NULL
        ) s
        ON (s.{seqColName} = t.{seqColName})
        WHEN MATCHED THEN UPDATE SET {updateColListStr}
        WHEN NOT MATCHED THEN INSERT ({seqColName}, {colList}) VALUES ({seqName}.nextval, {sColList})
        """
        cmdStr = cmdStr.strip().replace('\n',' ')

        cmdStr = re.sub('  +', ' ', cmdStr)

        cmd.append(SQLCommand(sqlCommand=cmdStr))

        ## Now truncate interim table
        # cmd.append(TruncateAction(interimTableName))

        return cmd
