import abc
from typing import List, Dict


class FrameworkMetaData(abc.ABC):
    @abc.abstractclassmethod
    def getMetaData(self) -> List[Dict]:
        pass
