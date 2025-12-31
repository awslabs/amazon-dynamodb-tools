import json
import os
import random
import string

import boto3
import botocore
from botocore.exceptions import ClientError

from clients import Clients
from infrastructure.verifier import is_existing_glue_job
from utils import module_zipper
from utils.logger import log

from __version__ import __version__ as VERSION

# project files
from .constants import (
    GLUE_JOB_NAME,
    GLUE_JOB_ROOT_ROLE_NAME,
    GLUE_JOB_SERVER_ROOT_PATH,
    GLUE_LOG_GROUP_NAMES,
    GLUE_LOG_GROUP_RETENTION_IN_DAYS,
    GLUE_VERSION,
    GlueJobDefaults,
    LOG4J_PROPERTIES_FILE,
    PYTHON_MODULE_CLIENT_ZIP_PATH,
    PYTHON_MODULE_SERVER_ZIP_PATH,
    PYTHON_VERSION,
    READ_ONLY_ROLE_ID,
    READ_WRITE_ROLE_ID,
    ROLE_TYPE_CUSTOM,
    ROLE_TYPE_READ_ONLY,
    ROLE_TYPE_READ_WRITE,
    READ_WRITE_ROLE_TYPES,
    THIRD_PARTY_PYTHON_MODULES,
)


class BootstrapInfrastructure:
    def __init__(self, env_configs):
        self.aws_region = env_configs.aws_region
        self.aws_account_id = env_configs.aws_account_id

        clients = Clients(self.aws_region)
        self.iam_client = clients.iam_client
        self.s3_client = clients.s3_client
        self.glue_client = clients.glue_client
        self.logs_client = clients.logs_client

    def _get_role_name(self, args):
        """
        Determine the appropriate role name based on the provided arguments.
        
        Args:
            args: Dictionary containing command line arguments
            
        Returns:
            str: The determined role name
        """
        role_param = args.get('XRole', '')

        # Check if a custom role was provided
        if role_param and role_param not in READ_WRITE_ROLE_TYPES:
            # Custom role name provided
            if not self._is_existing_role(role_param):
                print(f"Provided --XRole '{role_param}' name does not exist!")
                exit(1)
            return role_param

        # Handle standard role types
        is_write_access = role_param == ROLE_TYPE_READ_WRITE
        role_id = READ_WRITE_ROLE_ID if is_write_access else READ_ONLY_ROLE_ID
        return f"{GLUE_JOB_ROOT_ROLE_NAME}-{role_id}-{self.aws_region}" # region definition for separate region specific permissioning

    def _add_glue_job_role(self, args):
        log.info("Adding Glue Job role...")
        self._prompt_for_role(args)
        role_name = self._get_role_name(args)

        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "glue.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }

        pricing_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "pricing:GetProducts"
                    ],
                    "Resource": "*"
                }
            ]
        }

        quotas_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "servicequotas:GetServiceQuota",
                        "servicequotas:GetAWSDefaultServiceQuota"
                    ],
                    "Resource": "arn:aws:servicequotas:*:*:dynamodb/*"
                }
            ]
        }

        # Create the role
        try:
            response = self.iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy)
            )

            log.info(f"Bulk Executor Glue Job Role created: {role_name}")
            log.debug(f'Role ARN: {response["Role"]["Arn"]}')
        except self.iam_client.exceptions.EntityAlreadyExistsException as e:
            log.info(f"Found Bulk Executor Glue Job Role: {role_name}")
            return # Roles exists. No additional actions needed.
        except Exception as e:
            log.error(f'Unexpected error: {e}')
            exit(1)

        policy_arns = [
            'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole', # S3 permissions etc are handled here
        ]

        if self._is_write_access_enabled(args):
            policy_arns.append('arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess')
        else:
            policy_arns.append('arn:aws:iam::aws:policy/AmazonDynamoDBReadOnlyAccess')

        # Attach the policies to the role
        try:
            for policy_arn in policy_arns:
                self.iam_client.attach_role_policy(
                    RoleName=role_name,
                    PolicyArn=policy_arn
                )
                log.debug(f'Attached policy {policy_arn} to role {role_name}')

            # Give permissions accessing AWS services pricing
            self.iam_client.put_role_policy(
                RoleName=role_name,
                PolicyName='MinimalPricingAccess',
                PolicyDocument=json.dumps(pricing_policy)
            )
            log.debug(f'Attached pricing policy to role {role_name}')

            # Give permissions to access service quotas
            self.iam_client.put_role_policy(
                RoleName=role_name,
                PolicyName='MinimalQuotasAccess',
                PolicyDocument=json.dumps(quotas_policy)
            )
            log.debug(f'Attached quotas policy to role {role_name}')
        except Exception as e:
            log.error(f'Unexpected error: {e}')
            exit(1)

    def _is_existing_role(self, role_name):
        try:
            self.iam_client.get_role(RoleName=role_name)
            return True
        except self.iam_client.exceptions.NoSuchEntityException:
            return False
        except Exception as e:
            log.error(f'Unexpected error when checking for existing IAM Role: {e}')
            exit(1)

    def _create_or_update_glue_job(self, args, is_create_allowed=True):
        glue_job_bucket = self._get_glue_job_bucket_name()

        # Determine the role name
        glue_job_role_name = self._get_role_name(args)
        role_arn = f"arn:aws:iam::{self.aws_account_id}:role/{glue_job_role_name}"

        log.info(f"Attaching Glue Job Role {role_arn} to Bulk Executor Glue Job...")

        s3_script_location = f's3://{glue_job_bucket}/{GLUE_JOB_SERVER_ROOT_PATH}'
        s3_python_module_location = f's3://{glue_job_bucket}/{PYTHON_MODULE_SERVER_ZIP_PATH}'

        log4j2_properties_file_location = f's3://{glue_job_bucket}/{LOG4J_PROPERTIES_FILE}'

        default_arguments = {}

        # Add XEnvironmentArguments to be used by the Glue Job
        for key, value in args.items():
            if key.startswith('X') and key not in ('XRole', 'XRegion'):
                default_arguments[f'--{key}'] = str(value)

        default_arguments.update({ # Update last intentional.
            '--job-bookmark-option': 'job-bookmark-disable',
            '--enable-auto-scaling': 'true',
            '--enable-metrics': 'true',
            '--enable-observability-metrics': 'true',
            '--enable-continuous-cloudwatch-log': 'false', # Disabled due to custom log4j2.properties.
            '--glue-job-role-name': glue_job_role_name,
            '--s3-bucket-name': glue_job_bucket,
            '--s3-script-location': s3_script_location,
            '--extra-files': f'{log4j2_properties_file_location}',
            '--extra-py-files': s3_python_module_location,
            '--additional-python-modules': THIRD_PARTY_PYTHON_MODULES,
            '--bulk-dynamodb-version': VERSION
        })

        log.debug(f"default_arguments: {default_arguments}")

        try:
            if is_existing_glue_job(self.glue_client):
                log.debug("Updating Glue Job...")
                self.glue_client.update_job(
                    JobName=GLUE_JOB_NAME,
                    JobUpdate={
                        'Role': role_arn,
                        'Command': {
                            'Name': 'glueetl',
                            'PythonVersion': PYTHON_VERSION,
                            'ScriptLocation': s3_script_location,
                        },
                        'GlueVersion': GLUE_VERSION,
                        'NumberOfWorkers': args.get('XNumberOfWorkers', GlueJobDefaults.NumberOfWorkers.value),
                        'WorkerType': args.get('XWorkerType', GlueJobDefaults.WorkerType.value),
                        'Timeout': args.get('XTimeout', GlueJobDefaults.Timeout.value), # Configuration expects minutes
                        'MaxRetries': args.get('XRetries', GlueJobDefaults.Retries.value),
                        'DefaultArguments': default_arguments,
                        'ExecutionProperty': {
                            'MaxConcurrentRuns': args.get('XMaxConcurrentRuns', GlueJobDefaults.MaxConcurrentRuns.value),
                        }
                    }
                )
                log.info('Bulk Executor Glue Job updated successfully.')
            elif is_create_allowed:
                log.debug("Creating Glue Job...")
                self.glue_client.create_job(
                    Name=GLUE_JOB_NAME,
                    Role=role_arn,
                    Command={
                        'Name': 'glueetl',
                        'PythonVersion': PYTHON_VERSION,
                        'ScriptLocation': s3_script_location,
                    },
                    GlueVersion=GLUE_VERSION,
                    NumberOfWorkers=args.get('XNumberOfWorkers', GlueJobDefaults.NumberOfWorkers.value),
                    WorkerType=args.get('XWorkerType', GlueJobDefaults.WorkerType.value),
                    Timeout=args.get('XTimeout', GlueJobDefaults.Timeout.value),
                    MaxRetries=args.get('XRetries', GlueJobDefaults.Retries.value),
                    DefaultArguments=default_arguments,
                    ExecutionProperty={
                        'MaxConcurrentRuns':args.get('XMaxConcurrentRuns', GlueJobDefaults.MaxConcurrentRuns.value),
                    }
                )
                log.info('Bulk Executor Glue Job created successfully.')
            else:
                log.info('Bulk Executor Glue Job cannot be created!')
        except Exception as e:
            log.error(f"Error creating or updating Glue job: {e}")
            exit(1)

    def _bucket_exists(self, s3_client, bucket_name):
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            return True
        except boto3.exceptions.botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] in ("403", "404"):
                return False
            raise  # unexpected error

    def _upload_job_root_to_s3(self):
        glue_job_bucket = self._get_glue_job_bucket_name()

        # Check if the bucket exists
        if not self._bucket_exists(self.s3_client, glue_job_bucket):
            try:
                # Create the bucket
                bucket_config = {}
                if self.aws_region != 'us-east-1': # Default is us-east-1 so LocationConstraint fails if configured for this region.
                    bucket_config['CreateBucketConfiguration'] = {'LocationConstraint': self.aws_region}
                self.s3_client.create_bucket(
                    Bucket=glue_job_bucket,
                    **bucket_config
                )
                log.info(f"Bucket '{glue_job_bucket}' created successfully!")
            except Exception as e:
                log.error(f"Error creating bucket '{glue_job_bucket}': {e}")
                exit(1)
        else:
            log.info(f"Bucket '{glue_job_bucket}' already exists.")

        # Apply the secure transport policy
        try:
            secure_transport_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Deny",
                        "Principal": "*",
                        "Action": "s3:*",
                        "Resource": [
                            f"arn:aws:s3:::{glue_job_bucket}/*",
                            f"arn:aws:s3:::{glue_job_bucket}"
                        ],
                        "Condition": {
                            "Bool": {
                                "aws:SecureTransport": "false"
                            }
                        }
                    }
                ]
            }

            # Apply the bucket policy
            self.s3_client.put_bucket_policy(
                Bucket=glue_job_bucket,
                Policy=json.dumps(secure_transport_policy)
            )
            log.debug(f"Secure transport policy applied to bucket '{glue_job_bucket}'")
            
        except Exception as e:
            log.error(f"Unexpected error while applying SSL bucket policy: {e}")
            exit(1)

        self.s3_client.upload_file(f"./{GLUE_JOB_SERVER_ROOT_PATH}", glue_job_bucket, GLUE_JOB_SERVER_ROOT_PATH)
        log.info(f"Glue script '{GLUE_JOB_SERVER_ROOT_PATH}' uploaded into S3 successfully.")

    def _get_glue_job_bucket_name(self):
        # Return the existing persisted S3 Bucket name
        job_details = self._get_glue_job_details()
        if job_details:
            bucket_name = job_details['Job']['DefaultArguments'].get('--s3-bucket-name')
            if bucket_name:
                log.debug(f"S3 Bucket name found: {bucket_name}")
                return bucket_name

        # Create a new S3 Bucket
        suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=9)) # Generate a random 9-character suffix
        bucket_name = f"aws-glue-bulk-dynamodb-{self.aws_region}-{self.aws_account_id}-{suffix}" # `aws-glue-` prefix required by AWSGlueServiceRole
        log.debug(f"New S3 Bucket name generated: {bucket_name}")
        return bucket_name

    def _get_glue_job_details(self):
        try:
            return self.glue_client.get_job(JobName=GLUE_JOB_NAME)
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                log.debug('Bulk Executor Glue Job does not exist yet.')
                return None
            else:
                log.error(f"Unexpected error while checking for Glue Job details: {e}")
                exit(1)

    def _create_python_modules_archive(self):
        # Get the current working directory
        working_dir = os.path.dirname(os.path.abspath(__file__))

        # Create the 'tmp' directory
        os.makedirs(f"{working_dir}/tmp", exist_ok=True)

        # Write Files to Temp Directory
        module_zipper.zip_module() # Zip the Python Modules

    def update_python_modules_in_s3(self):
        self._create_python_modules_archive()
        glue_job_bucket = self._get_glue_job_bucket_name()
        self.s3_client.upload_file(f"./{PYTHON_MODULE_CLIENT_ZIP_PATH}", glue_job_bucket, PYTHON_MODULE_SERVER_ZIP_PATH)
        log.info(f"Python modules archive '{PYTHON_MODULE_CLIENT_ZIP_PATH}' uploaded into S3 successfully to '{PYTHON_MODULE_SERVER_ZIP_PATH}'.")

    def _prompt_for_role(self, args):
        """
        Prompt the user to select a role type if not already specified.
        Updates the args dictionary with the selected role.
        """
        role_param = args.get('XRole', '')

        if role_param in READ_WRITE_ROLE_TYPES:
            log.info(f"{role_param} role pre-configured.")
        elif role_param:
            log.info(f"Custom Glue Job Role detected: {args.get('XRole')}")
        else:
            # No role specified, prompt the user interactively
            log.info("No role specified. Please choose a role type:")
            log.info(f"  1. {ROLE_TYPE_READ_ONLY}     : Creates a read-only role (safer, but prevents operations like 'fill' that require write access)")
            log.info(f"  2. {ROLE_TYPE_READ_WRITE}    : Creates a role with write access (required for operations like 'fill')")
            log.info(f"  3. {ROLE_TYPE_CUSTOM}        : Use an existing IAM role name with appropriate permissions. See documentation for details.")

            role_choices = [
                "1", "2", "3", ROLE_TYPE_READ_ONLY.lower(), ROLE_TYPE_READ_WRITE.lower(), ROLE_TYPE_CUSTOM.lower()
            ]

            # Get user input
            choice = ""
            while choice not in role_choices:
                try:
                    choice = input(f"Enter your choice (1/2/3 or {ROLE_TYPE_READ_ONLY}/{ROLE_TYPE_READ_WRITE}/{ROLE_TYPE_CUSTOM}): ").strip().lower()
                    if choice not in role_choices:
                        print(f"Invalid choice. Please enter 1, 2, 3, {ROLE_TYPE_READ_ONLY}, {ROLE_TYPE_READ_WRITE}, or {ROLE_TYPE_CUSTOM}.")
                except EOFError:
                    # Handle non-interactive environments
                    log.error("Cannot prompt for role type in non-interactive mode.")
                    log.info("Please provide a role type using --XRole parameter.")
                    log.info(f"  --XRole {ROLE_TYPE_READ_ONLY}     : Creates a role with read-only access (safer, but prevents operations like 'delete' and 'fill' that require write access)")
                    log.info(f"  --XRole {ROLE_TYPE_READ_WRITE}    : Creates a role with read and write access (required for operations like 'delete' and 'fill')")
                    log.info(f"  --XRole {ROLE_TYPE_CUSTOM}        : Uses your own pre-defined IAM role name")
                    exit(1)

            # Process the choice and update args
            if choice in ["1", ROLE_TYPE_READ_ONLY.lower()]:
                log.info("Selected role with read-only access")
                args['XRole'] = ROLE_TYPE_READ_ONLY
            elif choice in ["2", ROLE_TYPE_READ_WRITE.lower()]:
                log.info("Selected role with read and write access")
                args['XRole'] = ROLE_TYPE_READ_WRITE
            elif choice in ["3", ROLE_TYPE_CUSTOM.lower()]:
                # Prompt for custom role name
                custom_role = ""
                while not custom_role:
                    try:
                        custom_role = input("Enter the name of your custom IAM role: ").strip()
                        if not custom_role:
                            print("Role name cannot be empty. Please enter a valid role name.")
                    except EOFError:
                        log.error("Cannot prompt for custom role name in non-interactive mode.")
                        exit(1)

                # Verify the role exists
                if not self._is_existing_role(custom_role):
                    log.error(f"The specified role '{custom_role}' does not exist!")
                    exit(1)

                log.info(f"Selected: custom role '{custom_role}'")
                args['XRole'] = custom_role

    def _is_write_access_enabled(self, args):
        """
        Determine if write access is enabled based on the role parameter.
        
        Args:
            args: Dictionary containing command line arguments
            
        Returns:
            bool: True if write access is enabled, False otherwise
        """
        role_param = args.get('XRole', '')

        # For custom roles, we don't determine access level here
        if role_param and role_param not in READ_WRITE_ROLE_TYPES:
            return None

        # For standard roles, determine access level
        return role_param == ROLE_TYPE_READ_WRITE

    def _upload_property_files_to_s3(self):
        glue_job_bucket = self._get_glue_job_bucket_name()
        self.s3_client.upload_file(f"./{LOG4J_PROPERTIES_FILE}", glue_job_bucket, LOG4J_PROPERTIES_FILE)
        log.info(f"Properties files '{LOG4J_PROPERTIES_FILE}' uploaded into S3 successfully!")

    def _create_glue_log_groups(self):
        """
        Create CloudWatch log groups for Glue job logging ahead of time.
        This prevents the need to wait for log groups to be created during job execution.
        """
        log.info("Creating CloudWatch log groups for Glue job...")
        
        for log_group_name in GLUE_LOG_GROUP_NAMES:
            try:
                # Try to create the log group - AWS will tell us if it already exists
                self.logs_client.create_log_group(logGroupName=log_group_name)
                log.info(f"Created log group: {log_group_name}")

                self.logs_client.put_retention_policy(
                    logGroupName=log_group_name,
                    retentionInDays=GLUE_LOG_GROUP_RETENTION_IN_DAYS
                )
                log.info(f"Set retention policy for {log_group_name} to {GLUE_LOG_GROUP_RETENTION_IN_DAYS} days")

            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceAlreadyExistsException':
                    log.info(f"Log group '{log_group_name}' already exists.")
                    self.logs_client.put_retention_policy(
                        logGroupName=log_group_name,
                        retentionInDays=GLUE_LOG_GROUP_RETENTION_IN_DAYS
                    )
                    log.info(f"Updated retention policy for existing log group {log_group_name}")
                else:
                    raise e # Handle failure case for all other errors at the higher level catch
            except Exception as e:
                log.error(f"Unexpected error creating log group '{log_group_name}': {e}")
                exit(1)

    def bootstrap(self, args):
        self._add_glue_job_role(args)
        self._create_glue_log_groups()
        self._create_or_update_glue_job(args)
        self._upload_job_root_to_s3()
        self.update_python_modules_in_s3()
        self._upload_property_files_to_s3()
