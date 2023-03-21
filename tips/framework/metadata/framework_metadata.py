import abc
from typing import List, Dict
from snowflake.snowpark import Session

class FrameworkMetaData(abc.ABC):

    @abc.abstractclassmethod
    def getMetaData(self, session: Session) -> List[Dict]:
        pass