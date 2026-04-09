from enum import Enum

class ImportType(Enum):
    FULL = "FULL_EXPORT"
    INCREMENTAL = "INCREMENTAL_EXPORT"


class Operation(str, Enum):
    PUT = "PUT"
    DELETE = "DELETE"

VALID_OPERATIONS = {Operation.PUT.value, Operation.DELETE.value}
