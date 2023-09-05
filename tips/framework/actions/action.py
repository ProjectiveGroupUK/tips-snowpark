from typing import List
import abc


class Action(abc.ABC):
    @abc.abstractclassmethod
    def getCommands() -> List[object]:
        pass

    @abc.abstractclassmethod
    def getBinds() -> List[str]:
        pass
