from snowflake.snowpark import Session

class Globals:
    _instance = None
    _session: Session
    _targetDatabase: str
    _callerId: str
    _sqlExecutionSequence: int

    """
    This is singleton class, hence on instantiation, it returns the same instance
    if already instantiated, otherwise creates a new instance.
    This is to enable reusing setter and getter methods across the project
    """

    def __new__(self):
        if self._instance is None:
            self._instance = super(Globals, self).__new__(self)
            # Put any initialization here.
            self._session = None
            self._targetDatabase = None
            self._sqlExecutionSequence = 0
        return self._instance

    def setSession(self, session: Session) -> None:
        self._session = session

    def getSession(self) -> Session:
        return self._session
    
    def setTargetDatabase(self, targetDatabase: str) -> None:
        self._targetDatabase = targetDatabase

    def getTargetDatabase(self) -> str:
        return self._targetDatabase    
    
    def setCallerId(self, callerId: str) -> None:
        self._callerId = callerId

    def getCallerId(self) -> str:
        return self._callerId

    def isNotCalledFromNativeApp(self) -> bool:
        if self._callerId == 'NativeApp':
            return False
        else:
            return True        
        
    def setSQLExecutionSequence(self, sqlExecutionSequence: int) -> None:
        self._sqlExecutionSequence = sqlExecutionSequence

    def getSQLExecutionSequence(self) -> int:
        return self._sqlExecutionSequence    
