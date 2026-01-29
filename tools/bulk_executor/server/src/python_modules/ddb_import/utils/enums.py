from enum import Enum

class ImportType(Enum):
    FULL_ONLY = "full-only"
    INCREMENTAL_ONLY = "incremental-only"
    FULL_INCREMENTAL = "full-incremental"
