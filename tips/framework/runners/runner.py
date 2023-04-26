import abc
from typing import Dict, List

from tips.framework.actions.action import Action

class Runner(abc.ABC):

    @abc.abstractclassmethod
    def execute(action: Action, frameworkRunner):
        pass