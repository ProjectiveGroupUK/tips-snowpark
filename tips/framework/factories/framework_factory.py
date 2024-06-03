from tips.framework.runners.sql_framework_metadata_runner import (
    SQLFrameworkMetaDataRunner,
)


class FrameworkFactory:
    def getProcess(self, processName: str):
        return SQLFrameworkMetaDataRunner(processName=processName)

    def getProcessStep(self, processName: str, processCmdId: int = 0):
        return SQLFrameworkMetaDataRunner(
            processName=processName, processCmdID=processCmdId
        )
