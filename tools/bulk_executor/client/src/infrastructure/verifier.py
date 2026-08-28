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

def _remote_is_newer(remote, local):
    """Is `remote` a newer version than `local`? None when undecidable.

    The version scheme is a plain incrementing integer ('4' at time of writing),
    so int() comparison is right for every version that has ever shipped. But the
    remote value comes from a *deployed* Glue job's DefaultArguments -- whatever
    client bootstrapped it wrote that string -- so it is not ours to trust. A
    fork, a hand-edited job, or a future semver-style scheme yields something
    like '4.1', and `int('4.1')` raises ValueError.

    That mattered because the crash replaced a genuinely useful message ("go
    re-bootstrap") with "invalid literal for int() with base 10", at exactly the
    moment the user needed the useful one. Returning None lets the caller give
    advice covering both directions instead.
    """
    try:
        return int(remote) > int(local)
    except (TypeError, ValueError):
        return None


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
                remote_is_newer = _remote_is_newer(
                    remote_bulk_dynamodb_version, local_bulk_dynamodb_version
                )
                if remote_is_newer is True:
                    message += "\nYou should probably upgrade the local client to match the higher version that was used for bootstrapping."
                elif remote_is_newer is False:
                    message += "\nYou should probably get a new bootstrap performed to upgrade the server-side to match the higher version on the client! If that's not possible, you could also downgrade your local version to match the lower version that was used for bootstrapping."
                else:
                    # Undecidable: one of the versions isn't a plain integer, so
                    # we can't say which side is ahead. Give both remedies rather
                    # than guessing -- or crashing, which is what comparing them
                    # with int() used to do.
                    message += (
                        "\nCould not tell which side is newer. Either upgrade the "
                        "local client to match the version used for bootstrapping, "
                        "or get a new bootstrap performed to bring the server-side "
                        "up to the local version."
                    )
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
