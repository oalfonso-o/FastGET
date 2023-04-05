class PatataError(Exception):
    pass


class ClientAlreadyInUseError(PatataError):
    pass


class InternalPatataError(PatataError):
    pass


class InvalidMethodError(PatataError):
    pass


class InvalidVerboseLevelError(PatataError):
    pass
