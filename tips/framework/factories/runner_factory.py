from tips.framework.actions.action import Action
from tips.framework.runners.sql_runner import SQLRunner


class RunnerFactory():

    def getRunner(self, action: Action):
        return SQLRunner()
