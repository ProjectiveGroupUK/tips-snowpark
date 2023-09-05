import json
from argparse import Action
from typing import Dict, List
from tips.framework.factories.action_factory import ActionFactory
from tips.framework.factories.runner_factory import RunnerFactory
from tips.framework.metadata.action_metadata import ActionMetadata
from tips.framework.metadata.additional_field import AdditionalField
from tips.framework.metadata.framework_metadata import FrameworkMetaData
from tips.framework.metadata.table_metadata import TableMetaData
from tips.framework.runners.runner import Runner
from tips.framework.utils.globals import Globals


class FrameworkRunner:
    _processName: str
    _bindVariables: Dict
    _executeFlag: str
    _globalsInstance: Globals

    returnJson: Dict = dict()
    dqTestLogList: List

    def __init__(self, processName: str, bindVariables: Dict, executeFlag: str) -> None:
        self._processName = processName
        self._bindVariables = bindVariables
        self._executeFlag = executeFlag
        self.returnJson = {
            "status": "NO EXECUTE" if self._executeFlag != "Y" else "SUCCESS",
            "error_message": str(),
            "warning_message": str(),
            "session_variables": self._bindVariables,
            "process": self._processName,
            "execute": self._executeFlag,
            "steps": [],
        }
        self.dqTestLogList = []
        self._globalsInstance = Globals()

    def isExecute(self) -> bool:
        return True if self._executeFlag == "Y" else False

    def run(
        self,
        tableMetaData: TableMetaData,
        frameworkMetaData: FrameworkMetaData,
        frameworkDQMetaData,
    ) -> Dict:
        session = self._globalsInstance.getSession()
        actionFactory: Action = ActionFactory()
        runnerFactory: Runner = RunnerFactory()

        for fwMetaData in frameworkMetaData:
            if fwMetaData["ACTIVE"] == "Y":
                # Additional fields can be pipe delimited. Within pipledelimited values, individual values
                # contain expression and column alias delimited with a space
                additionalFields: List[AdditionalField] = list()
                for fld in (
                    fwMetaData["ADDITIONAL_FIELDS"]
                    if fwMetaData["ADDITIONAL_FIELDS"] is not None
                    else ""
                ).split("|"):
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
                cmdBinds = (
                    []
                    if fwMetaData["CMD_BINDS"] is None
                    else [x.strip() for x in fwMetaData["CMD_BINDS"].split("|")]
                )
                ##If process accepts bind variables and no bind variables are passed in arguments then raise error
                if len(cmdBinds) > 0 and (
                    self._bindVariables == {}
                    or len(self._bindVariables) == 0
                    or len(self._bindVariables) < len(cmdBinds)
                ):
                    self.returnJson["status"] = "ERROR"
                    self.returnJson[
                        "error_message"
                    ] = f"Run time values for cmd_binds {fwMetaData['CMD_BINDS']} are expected but not received!!"
                    break

                for bind in cmdBinds:
                    if bind != "":
                        if bind not in self._bindVariables:
                            self.returnJson["status"] = "ERROR"
                            self.returnJson[
                                "error_message"
                            ] = f"cmd_binds {bind} doesnt exists in session_variables"
                            break

                        binds.append(self._bindVariables[bind])

                # Create Temp table - True or False
                tempTable = True if fwMetaData["TEMP_TABLE"] == "Y" else False

                # merge_on_fields is held as pipe delimited value
                mergeOnFields = [
                    x.strip()
                    for x in (
                        fwMetaData["MERGE_ON_FIELDS"]
                        if fwMetaData["MERGE_ON_FIELDS"] is not None
                        else ""
                    ).split("|")
                ]

                generateMergeMatchedClause = (
                    True
                    if fwMetaData["GENERATE_MERGE_MATCHED_CLAUSE"] == "Y"
                    else False
                )
                generateMergeWhenNotMatchedClause = (
                    True
                    if fwMetaData["GENERATE_MERGE_NON_MATCHED_CLAUSE"] == "Y"
                    else False
                )
                isActive = True if fwMetaData["ACTIVE"] == "Y" else False

                if fwMetaData["CMD_TYPE"] == "DQ_TEST":
                    cmdDQTests = frameworkDQMetaData[fwMetaData["PROCESS_CMD_ID"]]
                else:
                    cmdDQTests = []

                actionMetaData = ActionMetadata(
                    fwMetaData["CMD_TYPE"],
                    fwMetaData["CMD_SRC"] if fwMetaData["CMD_SRC"] is not None else "",
                    fwMetaData["CMD_TGT"] if fwMetaData["CMD_TGT"] is not None else "",
                    fwMetaData["CMD_WHERE"]
                    if fwMetaData["CMD_WHERE"] is not None
                    else "",
                    additionalFields,
                    binds,
                    tempTable,
                    fwMetaData["CMD_PIVOT_FIELD"],
                    fwMetaData["CMD_PIVOT_BY"],
                    fwMetaData["BUSINESS_KEY"]
                    if fwMetaData["BUSINESS_KEY"] is not None
                    else "",
                    fwMetaData["REFRESH_TYPE"],
                    mergeOnFields,
                    generateMergeMatchedClause,
                    generateMergeWhenNotMatchedClause,
                    isActive,
                    cmdDQTests,
                    fwMetaData["FILE_FORMAT_NAME"],
                    fwMetaData["COPY_INTO_FILE_PARITITION_BY"],
                    fwMetaData["PROCESS_CMD_ID"],
                )

                action = actionFactory.getAction(actionMetaData, tableMetaData, self)
                runner = runnerFactory.getRunner(action)
                ## Reset the sequence for sql statements within a process_cmd_id, so that sorting can be done on
                ## process_cmd_id and then order of execution of each sql within that process_cmd_id
                self._globalsInstance.setSQLExecutionSequence(sqlExecutionSequence=0)
                ret = runner.execute(action, self)
                ##If any of the steps failed, then break the loop and exit
                if ret == 1:
                    break

        return self.returnJson, self.dqTestLogList
