class AdditionalField():
    _expression: str
    _alias: str

    def __init__(self, expression: str, alias: str):
        self._expression = expression
        self._alias = alias

    def getField(self):
        return self._expression + " " + self._alias

    def getExpression(self):
        return self._expression

    def getAlias(self):
        return self._alias
