from argparse import Action
from typing import Dict, List
from snowflake.snowpark import Session
from tips.framework.factories.action_factory import ActionFactory
from tips.framework.factories.runner_factory import RunnerFactory
from tips.framework.metadata.action_metadata import ActionMetadata
from tips.framework.metadata.additional_field import AdditionalField
from tips.framework.metadata.framework_metadata import FrameworkMetaData
from tips.framework.metadata.table_metadata import TableMetaData
from tips.framework.runners.runner import Runner


class FrameworkRunner:
    _processName: str
    _bindVariables: Dict
    _executeFlag: str

    returnJson: Dict = dict()

    def __init__(self, processName: str, bindVariables: Dict, executeFlag: str) -> None:
        self._processName = processName
        self._bindVariables = bindVariables
        self._executeFlag = executeFlag
        self.returnJson = {
            "status": "NO EXECUTE" if self._executeFlag != "Y" else "SUCCESS",
            "error_message": str(),
            "session_variables": self._bindVariables,
            "process": self._processName,
            "execute": self._executeFlag,
            "steps": [],
        }

    def isExecute(self) -> bool:
        return True if self._executeFlag == 'Y' else False

    def run(
        self,
        session: Session,
        tableMetaData: TableMetaData,
        frameworkMetaData: FrameworkMetaData,
    ) -> Dict:

        actionFactory: Action = ActionFactory()
        runnerFactory: Runner = RunnerFactory()

        for fwMetaData in frameworkMetaData:

            # Additional fields can be pipe delimited. Within pipledelimited values, individual values
            # contain expression and column alias delimited with a space
            additionalFields: List[AdditionalField] = list()
            for fld in fwMetaData["ADDITIONAL_FIELDS"].split("|"):
                splittedField = fld.strip()
                # Now split column and alias
                if splittedField is not None and splittedField != "":

                    # In case expression and column alias have been delimited with multiple spaces, rather
                    # than a single space, remove any extra space characters first and then split on single space
                    fl = splittedField.replace("  ", " ").split(" ")
                    col = fl[0].strip()
                    alias = fl[1].strip()
                    additionalFields.append(AdditionalField(col, alias))

            # cmd_binds is held as pipe delimited value
            binds: List[str] = list()
            cmdBinds = [x.strip() for x in fwMetaData["CMD_BINDS"].split("|")]
            for bind in cmdBinds:
                if bind != "":
                    if bind not in self._bindVariables:
                        self.returnJson["status"] = "ERROR"
                        self.returnJson[
                            "error_message"
                        ] = f"cmd_binds {bind} doesnt exists in session_variables"
                        return self.returnJson

                    binds.append(self._bindVariables[bind])

            # Create Temp table - True or False
            tempTable = True if fwMetaData["TEMP_TABLE"] == "Y" else False

            # merge_on_fields is held as pipe delimited value
            mergeOnFields = [
                x.strip() for x in fwMetaData["MERGE_ON_FIELDS"].split("|")
            ]

            generateMergeMatchedClause = (
                True if fwMetaData["GENERATE_MERGE_MATCHED_CLAUSE"] == "Y" else False
            )
            generateMergeWhenNotMatchedClause = (
                True
                if fwMetaData["GENERATE_MERGE_NON_MATCHED_CLAUSE"] == "Y"
                else False
            )
            isActive = True if fwMetaData["ACTIVE"] == "Y" else False

            actionMetaData = ActionMetadata(
                fwMetaData["CMD_TYPE"],
                fwMetaData["CMD_SRC"],
                fwMetaData["CMD_TGT"],
                fwMetaData["CMD_WHERE"],
                additionalFields,
                binds,
                tempTable,
                list(),
                fwMetaData["BUSINESS_KEY"],
                fwMetaData["REFRESH_TYPE"],
                mergeOnFields,
                generateMergeMatchedClause,
                generateMergeWhenNotMatchedClause,
                isActive,
            )
            action = actionFactory.getAction(actionMetaData, tableMetaData, self)
            runner = runnerFactory.getRunner(action)
            ret = runner.execute(action, session, self)
            ##If any of the steps failed, then break the loop and exit
            if ret == 1:
                break

        return self.returnJson
