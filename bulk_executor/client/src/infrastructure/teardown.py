from botocore.exceptions import ClientError

# project files
from clients import Clients
from infrastructure.verifier import is_existing_glue_job
from utils.logger import log

from .constants import (
    GLUE_JOB_NAME,
    GLUE_JOB_ROOT_ROLE_NAME,
    READ_ONLY_ROLE_ID,
    READ_WRITE_ROLE_ID
)


class TeardownInfrastructure:
    def __init__(self, env_configs):
        self.aws_region = env_configs.aws_region

        clients = Clients(self.aws_region)
        self.iam_client = clients.iam_client
        self.s3_client = clients.s3_client
        self.glue_client = clients.glue_client

    def _get_glue_job_role_name(self):
        job_details = {}
        try:
            job_details = self.glue_client.get_job(JobName=GLUE_JOB_NAME)
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                log.warn('Bulk Executor Glue Job does not exist!') # Warn since this is expected behavior if teardown is executed twice consecutively.
                exit(1)
            else:
                log.error(f"Unexpected error while getting Glue Job Role name: {e}")
                exit(1)

        # Return the existing persisted Glue Job Role name
        glue_job_role_name = job_details['Job']['DefaultArguments']['--glue-job-role-name']
        log.debug(f"Glue Job role name found: {glue_job_role_name}")
        return glue_job_role_name

    def _delete_glue_job(self):
        try:
            if is_existing_glue_job(self.glue_client):
                log.debug("Deleting Glue Job...")
                self.glue_client.delete_job(
                    JobName=GLUE_JOB_NAME
                )
                log.info('Bulk Executor Glue Job deleted successfully.')
            else:
                log.info('Bulk Executor Glue Job cannot be deleted since it does not exist.')
        except Exception as e:
            log.error(f"Error creating or updating Glue job: {e}")
            exit(1)

    def _has_default_role_name(self, role_name):
        default_roles = set([
            f"{GLUE_JOB_ROOT_ROLE_NAME}-{READ_ONLY_ROLE_ID}-{self.aws_region}",
            f"{GLUE_JOB_ROOT_ROLE_NAME}-{READ_WRITE_ROLE_ID}-{self.aws_region}"
        ])
        return role_name in default_roles

    def _delete_glue_job_role(self):
        role_name = self._get_glue_job_role_name()

        if not self._has_default_role_name(role_name):
            log.info(f"Custom Glue Job Role detected and will NOT be deleted: {role_name}")
            return

        log.debug(f"Deleting role {role_name}...")

        managed_policies = []

        # Verify the Role Exists and Get Attached Policies
        try:
            managed_policies = self.iam_client.list_attached_role_policies(RoleName=role_name)['AttachedPolicies']
        except self.iam_client.exceptions.NoSuchEntityException:
            log.info(f"The role '{role_name}' does not exist.")
            return # Early return intentional.
        except Exception as e:
            log.error(f'Unexpected error listing policies for role {role_name}: {e}')
            exit(1)

        # Detach all managed policies from the role
        for policy in managed_policies:
            try:
                self.iam_client.detach_role_policy(RoleName=role_name, PolicyArn=policy['PolicyArn'])
            except Exception as e:
                log.error(f"Unexpected error detaching policy {policy['PolicyArn']}: {e}")
                exit(1)

        # Delete the role
        try:
            response = self.iam_client.delete_role(RoleName= role_name)
            log.info(f"Bulk Executor Glue Job Role deleted: {role_name}")
        except Exception as e:
            log.error(f'Unexpected error deleting role {role_name}: {e}')
            exit(1)

    def _get_glue_job_bucket_name(self):
        # Return the existing persisted S3 Bucket name
        job_details = self._get_glue_job_details()
        if job_details:
            bucket_name = job_details['Job']['DefaultArguments'].get('--s3-bucket-name')
            if bucket_name:
                log.debug(f"S3 Bucket name found: {bucket_name}")
                return bucket_name
        return None

    def _get_glue_job_details(self):
        try:
            return self.glue_client.get_job(JobName=GLUE_JOB_NAME)
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                log.info('Bulk Executor Glue Job does not exist.')
                return None
            else:
                log.error(f"Unexpected error while checking for Glue Job details: {e}")
                exit(1)

    def _delete_glue_job_bucket_and_server_objects(self):
        """
        Deletes all 'server/' objects in the specified S3 bucket and attempts to delete the bucket itself.
        
        If additional objects exist in the bucket besides the 'server/' objects, the bucket is left intact
        for manual review and cleanup by the bucket owner via the Console. A warning is logged in this case.
        
        The teardown process continues regardless of whether bucket deletion succeeds or fails.
        When a new Bulk job is bootstrapped, a new bucket will be created (to prevent bucket sniping).
        """
        bucket_name = self._get_glue_job_bucket_name()
        if not bucket_name:
            log.warn("Unable to determine glue job bucket name! Has the Bulk Executor Glue Job already been deleted?")
            return
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix='server/')
        except self.s3_client.exceptions.NoSuchBucket:
            log.info("The Bulk Executor S3 Bucket cannot be deleted since it does not exist!")
            return # Early return intentional
        except Exception as e:
            log.error(f'Unexpected error listing attached S3 objects for S3 Bucket {bucket_name}: {e}')
            exit(1)

        if 'Contents' in response:
            object_keys = [content['Key'] for content in response['Contents']]

            batch_size = 1000
            for i in range(0, len(object_keys), batch_size):
                batch = object_keys[i:i+batch_size]

                delete_response = self.s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': [{'Key': key} for key in batch]}
                )
                if 'Deleted' in delete_response:
                    log.info(f'Deleted {len(delete_response["Deleted"])} objects from {bucket_name}')
                if 'Errors' in delete_response:
                    for error in delete_response['Errors']:
                        log.error(f"Error deleting {error['Key']}: {error['Message']} (Code: {error['Code']})")
                    exit(1)

        # Try to delete the bucket itself, but continue if it fails due to not being empty or missing
        try:
            bucket_response = self.s3_client.delete_bucket(Bucket=bucket_name)
            log.info(f'Bucket {bucket_name} has been deleted.')
        except self.s3_client.exceptions.NoSuchBucket:
            log.info("The Bulk Executor S3 Bucket cannot be deleted since it does not exist!")
            return # Early return intentional
        except self.s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'BucketNotEmpty':
                log.warn(f'Bucket {bucket_name} is not empty and could not be deleted. Continuing with teardown.')
            else:
                log.error(f'Unexpected error deleting bucket {bucket_name}: {e}')
                exit(1)

    def teardown(self):
        # Deletion order intentional
        self._delete_glue_job_bucket_and_server_objects()
        self._delete_glue_job_role()
        self._delete_glue_job()
