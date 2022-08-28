class InvalidTestError(Exception):
    pass

class InvalidTestConfigError(InvalidTestError):
    pass

class InvalidTestConnectionError(InvalidTestError):
    pass

class InvalidTestRuntimeError(InvalidTestError):
    pass