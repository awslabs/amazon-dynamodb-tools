from botocore.exceptions import ClientError

from __version__ import __version__ as VERSION

# project files
from .constants import (
    GLUE_JOB_NAME
)
from utils.logger import log


def _get_glue_job_details(glue_client):
    try:
        return glue_client.get_job(JobName=GLUE_JOB_NAME)
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            log.debug('Bulk Executor Glue Job does not exist!')
            return None
        else:
            log.error(f"Unexpected error while getting Glue Job details: {e}")
            exit(1)

def assert_version_parity(glue_client, args):
    job_details = _get_glue_job_details(glue_client)
    if job_details:
        remote_bulk_dynamodb_version = job_details['Job']['DefaultArguments'].get('--bulk-dynamodb-version')
        if remote_bulk_dynamodb_version:
            local_bulk_dynamodb_version = VERSION
            has_matching_versions = local_bulk_dynamodb_version == remote_bulk_dynamodb_version
            if not has_matching_versions:
                message = f"""
                Local and remote versions must match exactly! Local is {local_bulk_dynamodb_version}, remote is {remote_bulk_dynamodb_version}.
                """
                if int(remote_bulk_dynamodb_version) > int(local_bulk_dynamodb_version):
                    message += "\nYou should probably upgrade the local client to match the higher version that was used for bootstrapping."
                else:
                    message += "\nYou should probably get a new bootstrap performed to upgrade the server-side to match the higher version on the client! If that's not possible, you could also downgrade your local version to match the lower version that was used for bootstrapping."
                raise ValueError(message)
            return
    message = """
    Remote version not available! Unable to determine if local and remote versions match.
    If this error persists please contact whoever bootstrapped your environment.
    """
    raise ValueError(message)

def is_existing_glue_job(glue_client):
    try:
        response = glue_client.get_job(JobName=GLUE_JOB_NAME)
        return True
    except Exception as e:
        if hasattr(e, 'response') and e.response['Error']['Code'] == 'EntityNotFoundException':
            log.debug('Bulk Executor Glue Job does not exist!')
            return False
        else:
            log.error(f"Unexpected error while checking for existing Glue Job: {e}")
            exit(1)
