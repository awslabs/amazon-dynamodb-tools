from enum import Enum

GLUE_VERSION = '4.0'
PYTHON_VERSION = '3'
LOG4J_PROPERTIES_FILE = 'server/src/log4j2.properties'

GLUE_JOB_NAME = 'bulk_dynamodb'
GLUE_JOB_ROOT_ROLE_NAME = 'AWSGlueServiceRoleBulkDynamoDB' # AWSGlueServiceRole prefix intentional.
GLUE_JOB_SERVER_ROOT_PATH = "server/src/root.py"

# CloudWatch Log Groups for Glue Jobs
GLUE_LOG_GROUP_ERROR = '/aws-glue/jobs/error'
GLUE_LOG_GROUP_OUTPUT = '/aws-glue/jobs/output'
GLUE_LOG_GROUP_NAMES = [GLUE_LOG_GROUP_ERROR, GLUE_LOG_GROUP_OUTPUT]
GLUE_LOG_GROUP_RETENTION_IN_DAYS = 365 # One year

READ_ONLY_ROLE_ID = "DdbReadOnly"
READ_WRITE_ROLE_ID = "DdbReadWrite"

# Role type constants
ROLE_TYPE_READ_ONLY = 'READ-ONLY'
ROLE_TYPE_READ_WRITE = 'READ-WRITE'
ROLE_TYPE_CUSTOM = 'custom'
READ_WRITE_ROLE_TYPES = [ROLE_TYPE_READ_ONLY, ROLE_TYPE_READ_WRITE]  # Standard role types, excluding custom

PYTHON_MODULE_CLIENT_DIR_PATH = 'server/src/python_modules'
PYTHON_MODULE_CLIENT_ZIP_PATH = 'client/src/infrastructure/tmp/python_modules.zip'
PYTHON_MODULE_SERVER_ZIP_PATH = 'server/src/python_modules.zip'

class GlueJobDefaults(Enum):
    ExecutionClass='STANDARD'
    MaxConcurrentRuns=20
    Retries=0
    Timeout=60
    NumberOfWorkers=220
    WorkerType='G.1X'

# Third Party Dependencies as an alpha-numeric list
_THIRD_PARTY_PYTHON_MODULES = [
  'faker'
]

# Convert to AWS Glue Readable Format
THIRD_PARTY_PYTHON_MODULES = ','.join(map(str, _THIRD_PARTY_PYTHON_MODULES))
