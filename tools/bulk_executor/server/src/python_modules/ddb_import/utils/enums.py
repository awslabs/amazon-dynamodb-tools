from enum import Enum

class ImportType(Enum):
    FULL = "FULL_EXPORT"
    INCREMENTAL = "INCREMENTAL_EXPORT"
