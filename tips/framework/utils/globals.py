from snowflake.snowpark import Session

class Globals:
    _instance = None
    _session: Session

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
        return self._instance

    def setSession(self, session: Session) -> None:
        self._session = session

    def getSession(self) -> Session:
        return self._session