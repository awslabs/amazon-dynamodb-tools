from enum import Enum

GLUE_VERSION = '5.1'
PYTHON_VERSION = '3'
LOG4J_PROPERTIES_FILE = 'server/src/log4j2.properties'

GLUE_JOB_NAME = 'bulk_dynamodb'
GLUE_JOB_ROOT_ROLE_NAME = 'AWSGlueServiceRoleBulkDynamoDB' # AWSGlueServiceRole prefix intentional.
GLUE_JOB_SERVER_ROOT_PATH = "server/src/root.py"

# Glue 5.x DataFrame-based DynamoDB source requires an attached Glue
# connection of type DYNAMODB to register the data source on the Spark
# classpath. ConnectionProperties is intentionally empty; the connection
# is purely a marker that tells Glue to load the connector library.
GLUE_DYNAMODB_CONNECTION_NAME = 'bulk-dynamodb-connection'

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
    IdleTimeout=5
    NumberOfWorkers=220
    WorkerType='G.1X'

# Third Party Dependencies installed on every Glue run (alpha-numeric list).
# Verb-specific deps go in VERB_PYTHON_MODULES below.
_THIRD_PARTY_PYTHON_MODULES = []

# Convert to AWS Glue Readable Format
THIRD_PARTY_PYTHON_MODULES = ','.join(map(str, _THIRD_PARTY_PYTHON_MODULES))

# Dependencies needed only for specific verbs (keyed by verb name).
# These are added per-run via start_job_run Arguments to avoid
# installing them for every job invocation.
VERB_PYTHON_MODULES = {
    'fill': ['faker'],
}
