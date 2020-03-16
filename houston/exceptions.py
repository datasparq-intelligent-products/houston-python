"""Houston client exceptions"""


class HoustonException(Exception):
    """Base Houston client exception"""

    pass


class HoustonServerBusy(HoustonException):
    """Raised due to too many requests to process"""

    pass


class HoustonClientError(HoustonException):
    """Error raised when the client has made an invalid request"""

    pass


class HoustonServerError(HoustonException):
    """Error raised when Houston server has issues"""

    pass
