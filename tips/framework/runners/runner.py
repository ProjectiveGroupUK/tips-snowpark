import abc
from typing import Dict, List

from snowflake.snowpark import Session
from tips.framework.actions.action import Action

class Runner(abc.ABC):

    @abc.abstractclassmethod
    def execute(action: Action, session: Session, frameworkRunner):
        pass