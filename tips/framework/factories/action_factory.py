from typing import Dict
from tips.framework.actions.action import Action
from tips.framework.actions.append_action import AppendAction
from tips.framework.actions.copy_into_file_action import CopyIntoFileAction
from tips.framework.actions.copy_into_table_action import CopyIntoTableAction
from tips.framework.actions.default_action import DefaultAction
from tips.framework.actions.delete_action import DeleteAction
from tips.framework.actions.di_refresh_action import DIRefreshAction
from tips.framework.actions.merge_action import MergeAction
from tips.framework.actions.oi_refresh_action import OIRefreshAction
from tips.framework.actions.scd2_publish_action import SCD2PublishAction
from tips.framework.actions.ti_refresh_action import TIRefreshAction
from tips.framework.actions.truncate_action import TruncateAction
from tips.framework.actions.dq_test_action import DQTestAction
from tips.framework.metadata.action_metadata import ActionMetadata
from tips.framework.metadata.table_metadata import TableMetaData


# Below is to initialise logging
import logging
from tips.utils.logger import Logger

logger = logging.getLogger(Logger.getRootLoggerName())


class ActionFactory:
    def getAction(
        self, actionMetaData: ActionMetadata, metadata: TableMetaData, frameworkRunner
    ) -> Action:
        action: Action

        logger.info("Inside ActionFactory...")

        actionJson: Dict = {
            "status": "SKIPPED"
            if actionMetaData.isActive() == False
            else "NO EXECUTE"
            if frameworkRunner.isExecute() == False
            else "SUCCESS",
            "error_message": "",
            "warning_message": "",
            "process_cmd_id": actionMetaData.getProcessCmdId(),
            "action": actionMetaData.getCmdType(),
            "parameters": {
                "source": actionMetaData.getSource(),
                "target": actionMetaData.getTarget(),
                "where_clause": actionMetaData.getWhereClause(),
                "binds": actionMetaData.getBinds(),
                "temp_table": actionMetaData.isCreateTempTable(),
                "active": actionMetaData.isActive(),
            },
            "commands": [],
        }

        if actionMetaData.getCmdType() == "APPEND":
            logger.info("Running Append Action...")
            action = AppendAction(
                source=actionMetaData.getSource(),
                target=actionMetaData.getTarget(),
                whereClause=actionMetaData.getWhereClause(),
                metadata=metadata,
                binds=actionMetaData.getBinds(),
                additionalFields=actionMetaData.getAdditionalFields(),
                isOverwrite=False,
                isCreateTempTable=actionMetaData.isCreateTempTable(),
            )
        elif actionMetaData.getCmdType() == "COPY_INTO_FILE":
            logger.info("Running Copy Into File Action...")
            action = CopyIntoFileAction(
                actionMetaData.getSource(),
                actionMetaData.getTarget(),
                actionMetaData.getWhereClause(),
                actionMetaData.getBinds(),
                actionMetaData.getPivotBy(),
                actionMetaData.getPivotField(),
                actionMetaData.getFileFormatName(),
                actionMetaData.getCopyIntoFilePartitionBy(),
            )
        elif actionMetaData.getCmdType() == "COPY_INTO_TABLE":
            logger.info("Running Copy Into Table Action...")
            action = CopyIntoTableAction(
                source=actionMetaData.getSource(),
                target=actionMetaData.getTarget(),
                binds=actionMetaData.getBinds(),
                fileFormatName=actionMetaData.getFileFormatName(),
                additonalFields=actionMetaData.getAdditionalFields(),
                copyAutoMapping=actionMetaData.getCopyAutoMapping(),
                copyIntoForce=actionMetaData.getCopyIntoForce(),
                metadata=metadata
            )
        elif actionMetaData.getCmdType() == "DELETE":
            logger.info("Running Delete Action...")
            action = DeleteAction(
                actionMetaData.getSource(),
                actionMetaData.getWhereClause(),
                actionMetaData.getBinds(),
            )
        elif actionMetaData.getCmdType() == "MERGE":
            logger.info("Running Merge Action...")
            action = MergeAction(
                source=actionMetaData.getSource(),
                target=actionMetaData.getTarget(),
                whereClause=actionMetaData.getWhereClause(),
                metadata=metadata,
                binds=actionMetaData.getBinds(),
                additionalFields=actionMetaData.getAdditionalFields(),
                mergeOnFields=actionMetaData.getMergeOnFields(),
                generateMergeMatchedClause=actionMetaData.isGenerateMergeMatchedClause(),
                generateMergeWhenNotMatchedClause=actionMetaData.isGenerateMergeWhenNotMatchedClause(),
                isCreateTempTable=actionMetaData.isCreateTempTable(),
            )
        elif actionMetaData.getCmdType() == "PUBLISH_SCD2_DIM":
            logger.info("Running Publish SCD2 Dim Action...")
            action = SCD2PublishAction(
                source=actionMetaData.getSource(),
                target=actionMetaData.getTarget(),
                whereClause=actionMetaData.getWhereClause(),
                businessKey=actionMetaData.getBusinessKey(),
                metadata=metadata,
                binds=actionMetaData.getBinds(),
                additionalFields=actionMetaData.getAdditionalFields(),
                isCreateTempTable=actionMetaData.isCreateTempTable(),
            )
        elif actionMetaData.getCmdType() == "REFRESH":
            if actionMetaData.getRefreshType() == "DI":
                logger.info("Running DI Refresh Action...")
                action = DIRefreshAction(
                    actionMetaData.getSource(),
                    actionMetaData.getTarget(),
                    actionMetaData.getWhereClause(),
                    metadata,
                    actionMetaData.getBinds(),
                    actionMetaData.getAdditionalFields(),
                    isCreateTempTable=actionMetaData.isCreateTempTable(),
                )
            elif actionMetaData.getRefreshType() == "OI":
                logger.info("Running OI Refresh Action...")
                action = OIRefreshAction(
                    actionMetaData.getSource(),
                    actionMetaData.getTarget(),
                    actionMetaData.getWhereClause(),
                    metadata,
                    actionMetaData.getBinds(),
                    actionMetaData.getAdditionalFields(),
                    isCreateTempTable=actionMetaData.isCreateTempTable(),
                )
            elif actionMetaData.getRefreshType() == "TI":
                logger.info("Running TI Refresh Action...")
                action = TIRefreshAction(
                    actionMetaData.getSource(),
                    actionMetaData.getTarget(),
                    actionMetaData.getWhereClause(),
                    metadata,
                    actionMetaData.getBinds(),
                    actionMetaData.getAdditionalFields(),
                    isCreateTempTable=actionMetaData.isCreateTempTable(),
                )
            else:
                logger.info("Running Default Action...")
                action = DefaultAction()
        elif actionMetaData.getCmdType() == "TRUNCATE":
            logger.info("Running Truncate Action...")
            action = TruncateAction(actionMetaData.getTarget())
        elif actionMetaData.getCmdType() == "DQ_TEST":
            logger.info("Running DQ Test Action...")
            action = DQTestAction(
                cmdDQTests=actionMetaData.getCmdDQTests(),
                whereClause=actionMetaData.getWhereClause(),
                binds=actionMetaData.getBinds(),
            )
        else:
            logger.info("Running Default Action...")
            action = DefaultAction()

        frameworkRunner.returnJson["steps"].append(actionJson)

        return action
