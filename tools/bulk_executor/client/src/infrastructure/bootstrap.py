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
from utils.role_validator import FATAL, validate_custom_role_permissions

from __version__ import __version__ as VERSION

# project files
from .constants import (
    GLUE_DYNAMODB_CONNECTION_NAME,
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

        # Custom --XRole names whose permissions have already been validated
        # this run. bootstrap() resolves the role name twice (_add_glue_job_role
        # and _create_or_update_glue_job), and validation is now heavyweight
        # (IAM reads + SimulatePrincipalPolicy). Without this, the reads fire
        # twice and every WARNING is logged twice. See _get_role_name.
        self._validated_custom_roles = set()

        # Resources this run created, so a failed bootstrap can name what it
        # left behind (issue #307). teardown locates resources through the Glue
        # job, so if bootstrap dies before creating the job these are
        # unreachable by the supported path -- saying so beats a silent leak.
        self._role_created_this_run = None
        self._bucket_created_this_run = None

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
            # Validate each custom role at most once per run. A FATAL finding
            # exits below before the name is cached, so re-resolving after an
            # eject can't happen; a role that only warned (or was clean) skips
            # the second, redundant IAM read + SimulatePrincipalPolicy pass and
            # avoids logging the same WARNINGs twice.
            if role_param in self._validated_custom_roles:
                return role_param
            findings = validate_custom_role_permissions(self.iam_client, role_param)
            # Surface every finding, then abort if any is FATAL. FATAL means the
            # role provably cannot work (wrong name prefix, or a trust policy
            # that won't let Glue assume it), so we eject here -- before any
            # infrastructure is created -- rather than letting the operator hit
            # an opaque Glue failure later. WARNINGs are advisory and never block.
            fatal_found = False
            for finding in findings:
                if finding.severity == FATAL:
                    log.error(finding.message)
                    fatal_found = True
                else:
                    log.warning(finding.message)
            if fatal_found:
                print(
                    f"Provided --XRole '{role_param}' cannot be used by the Glue "
                    f"job (see the errors above). Aborting."
                )
                exit(1)
            # Reached only when no finding was FATAL: record the role so the
            # second resolution in this run doesn't re-validate or re-warn.
            self._validated_custom_roles.add(role_param)
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

        # Read-only visibility into a table's autoscaling configuration so the
        # job can tell whether autoscaling would lift a provisioned table's
        # ceiling above a user-requested rate (issue #89). DescribeScalingPolicies
        # is what supplies the target-utilization value in the autoscaling
        # diagnostic; without it the whole diagnostic degrades (issue #297).
        # Neither action supports resource-level scoping, so the resource must
        # be "*".
        autoscaling_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "application-autoscaling:DescribeScalableTargets",
                        "application-autoscaling:DescribeScalingPolicies"
                    ],
                    "Resource": "*"
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
            self._role_created_this_run = role_name
        except self.iam_client.exceptions.EntityAlreadyExistsException as e:
            log.info(f"Found Bulk Executor Glue Job Role: {role_name}")
            log.debug(f"Applying the current policy set to {role_name}")
            # Deliberately falls through to the policy work below: a
            # bootstrap-generated role is brought up to what *this* version wants
            # on every bootstrap, not only when __version__ changed.
            #
            # This used to be gated on _needs_role_refresh(), which compared the
            # deployed Glue job's version against the local one. That answered "is
            # the job stale?" when the question is "is this role provisioned the way
            # this version expects?", and it missed two real cases (issue #326):
            #
            #   - Re-bootstrapping with a different --XRole. Each role type is its
            #     own role (...-DdbReadOnly-... vs ...-DdbReadWrite-...), so the one
            #     you switch to may never have been touched since a version bump
            #     repaired the other -- yet version parity now holds, so it was
            #     skipped forever.
            #   - A role left half-provisioned by anything at all. CloudTrail showed
            #     an interrupted e2e security run creating the role and stopping
            #     after the two managed-policy attaches, so it sat with no inline
            #     policies. Every verb then died in the cost estimate with
            #     AccessDenied on pricing:GetProducts, and re-bootstrapping could not
            #     fix it because the version matched.
            #
            # Every call below is idempotent, so doing them unconditionally is both
            # cheap and self-repairing. The trust policy is re-applied here because
            # create_role -- which would have set it -- was skipped.
            try:
                self.iam_client.update_assume_role_policy(
                    RoleName=role_name,
                    PolicyDocument=json.dumps(trust_policy)
                )
                log.debug(f'Refreshed trust policy on role {role_name}')
            except Exception as e:
                log.error(f'Unexpected error: {e}')
                exit(1)
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

            # Give read-only access to autoscaling targets (issue #89 rate warnings)
            self.iam_client.put_role_policy(
                RoleName=role_name,
                PolicyName='MinimalAutoScalingAccess',
                PolicyDocument=json.dumps(autoscaling_policy)
            )
            log.debug(f'Attached autoscaling policy to role {role_name}')
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

        enable_continuous_cloudwatch_log = bool(args.get('XContinuousLogging'))

        default_arguments = {}

        # Add XEnvironmentArguments to be used by the Glue Job.
        # XRole, XRegion, and XAccount are bootstrap-time / client-side
        # concerns and are not needed at job runtime, so they are excluded
        # from DefaultArguments. See issue #85.
        for key, value in args.items():
            if key.startswith('X') and key not in ('XRole', 'XRegion', 'XAccount'):
                default_arguments[f'--{key}'] = str(value)

        default_arguments.update({ # Update last intentional.
            '--job-bookmark-option': 'job-bookmark-disable',
            '--enable-auto-scaling': 'true',
            '--enable-metrics': 'true',
            '--enable-observability-metrics': 'true',
            '--enable-continuous-cloudwatch-log': str(enable_continuous_cloudwatch_log).lower(),
            '--glue-job-role-name': glue_job_role_name,
            '--s3-bucket-name': glue_job_bucket,
            '--s3-script-location': s3_script_location,
            '--extra-py-files': s3_python_module_location,
            '--bulk-dynamodb-version': VERSION
        })

        # Custom log4j2 properties override continuous CloudWatch logging
        # Reference: https://repost.aws/knowledge-center/glue-reduce-cloudwatch-logs
        # "If you apply a custom log4j.properties or log4j2.properties config file, then AWS Glue turns off continuous logging"
        if not enable_continuous_cloudwatch_log:
            default_arguments['--extra-files'] = log4j2_properties_file_location

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
                        'Connections': {'Connections': [GLUE_DYNAMODB_CONNECTION_NAME]},
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
                    Connections={'Connections': [GLUE_DYNAMODB_CONNECTION_NAME]},
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
                self._bucket_created_this_run = glue_job_bucket
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

    def _get_log_group_retention(self, log_group_name):
        """Return the retentionInDays set on log_group_name, or None if unset.

        describe_log_groups takes a name *prefix* and can return multiple groups,
        so match the exact name rather than trusting the first result.
        """
        response = self.logs_client.describe_log_groups(logGroupNamePrefix=log_group_name)
        for group in response.get('logGroups', []):
            if group.get('logGroupName') == log_group_name:
                return group.get('retentionInDays')
        return None

    def _create_glue_log_groups(self):
        """
        Create CloudWatch log groups for Glue job logging ahead of time.

        We really prefer to create the log groups here proactively before the
        first Glue job run so during the first execution we can attach LiveTail
        immediately and not miss any early output. Creating them is therefore
        necessary, and a failure here is fatal.

        We politely try to set a retention policy if we have permissions, but if
        we can't then we'll let the default stand. An existing retention policy
        other than the default of None we leave alone. So retention failures warn
        and carry on (issues #294, #301).
        """
        log.info("Creating CloudWatch log groups for Glue job...")
        
        for log_group_name in GLUE_LOG_GROUP_NAMES:
            # --- necessary: the group itself must exist (see docstring) ---
            group_existed = False
            try:
                # Try to create the log group - AWS will tell us if it already exists
                self.logs_client.create_log_group(logGroupName=log_group_name)
                log.info(f"Created log group: {log_group_name}")
            except self.logs_client.exceptions.ResourceAlreadyExistsException:
                log.info(f"Log group '{log_group_name}' already exists.")
                group_existed = True
            except Exception as e:
                log.error(
                    f"Could not create log group '{log_group_name}': {e}. "
                    f"The Glue job streams its output through this log group, "
                    f"and bulk commands wait for it to exist before tailing -- "
                    f"without it, early job output is lost and commands can "
                    f"exit while waiting. Grant logs:CreateLogGroup and "
                    f"re-run bootstrap."
                )
                exit(1)

            # --- courtesy: retention is a default, never worth failing over ---
            # Only set retention if the group has none. If an account owner
            # deliberately chose a retention (e.g. 30 days for cost, or a longer
            # window for compliance), we must not clobber it on every bootstrap.
            # A group with no policy (e.g. auto-created by Glue, so "never
            # expire") still gets our default. Staying silent when we leave an
            # existing policy alone keeps the console clean.
            try:
                if not group_existed:
                    self.logs_client.put_retention_policy(
                        logGroupName=log_group_name,
                        retentionInDays=GLUE_LOG_GROUP_RETENTION_IN_DAYS
                    )
                    log.info(f"Set retention policy for {log_group_name} to {GLUE_LOG_GROUP_RETENTION_IN_DAYS} days")
                elif self._get_log_group_retention(log_group_name) is None:
                    self.logs_client.put_retention_policy(
                        logGroupName=log_group_name,
                        retentionInDays=GLUE_LOG_GROUP_RETENTION_IN_DAYS
                    )
                    log.info(f"Set retention policy for existing log group {log_group_name} to {GLUE_LOG_GROUP_RETENTION_IN_DAYS} days (had none)")
            except Exception as e:
                # Surface the underlying error (it names the denied operation) and
                # the consequence, so the warning is actionable rather than noise
                # (issues #294, #301).
                log.warning(
                    f"Could not manage the retention policy on log group "
                    f"'{log_group_name}' ({e}); continuing. Any retention already "
                    f"set is left untouched, and log capture is unaffected."
                )

    def bootstrap(self, args):
        try:
            self._add_glue_job_role(args)
            self._create_glue_log_groups()
            self._ensure_dynamodb_glue_connection()
            self._create_or_update_glue_job(args)
            self._upload_job_root_to_s3()
            self.update_python_modules_in_s3()
            self._upload_property_files_to_s3()
        except SystemExit:
            # Any step may exit(1). Report here rather than at each call site so
            # a new step can't forget to (issue #307).
            self._report_resources_left_behind()
            raise

    def _report_resources_left_behind(self):
        """Name anything this run created before bootstrap failed (issue #307).

        Only resources THIS run created are reported -- a role that already
        existed was not ours to leak. teardown resolves resources through the
        Glue job, so when bootstrap dies before creating the job it bails with
        "Unable to determine glue job bucket name" and never reaches them; the
        operator needs to know they exist and that teardown won't help.
        """
        leftovers = []
        if self._role_created_this_run:
            leftovers.append(f"IAM role '{self._role_created_this_run}'")
        if self._bucket_created_this_run:
            leftovers.append(f"S3 bucket '{self._bucket_created_this_run}'")
        if not leftovers:
            return

        # Agree in number: the common case is a single leftover (the role), and
        # "IAM role 'x', which have been left in place" reads like a bug in a
        # message whose whole job is to be trusted.
        has_have = "has" if len(leftovers) == 1 else "have"
        it_them = "it" if len(leftovers) == 1 else "them"
        log.error(
            f"Bootstrap did not complete. It had already created "
            f"{' and '.join(leftovers)}, which {has_have} been left in place. "
            f"Fix the error above and re-run bootstrap to reuse {it_them}. To "
            f"remove {it_them} instead, delete {it_them} manually -- 'bulk "
            f"teardown' finds resources through the Glue job, so it cannot clean "
            f"up when the job was never created."
        )

    def _ensure_dynamodb_glue_connection(self):
        """Create a Glue connection of type DYNAMODB if missing.

        Glue 5.0+ requires this connection to be attached to the job for
        the DataFrame-based DynamoDB source (spark.read.format("dynamodb"))
        to register on the Spark classpath. Without it, jobs invoking the
        new connector fail with "[DATA_SOURCE_NOT_FOUND] dynamodb".

        The connection itself carries no credentials -- ConnectionProperties
        is empty. It exists purely as a marker that tells Glue to load the
        DynamoDB DataFrame connector library on the executors.
        """
        connection_input = {
            'Name': GLUE_DYNAMODB_CONNECTION_NAME,
            'ConnectionType': 'DYNAMODB',
            'ConnectionProperties': {},
            'ValidateCredentials': False,
            'ValidateForComputeEnvironments': ['SPARK'],
        }
        try:
            self.glue_client.get_connection(Name=GLUE_DYNAMODB_CONNECTION_NAME)
            log.debug(f"Glue connection '{GLUE_DYNAMODB_CONNECTION_NAME}' already exists.")
        except self.glue_client.exceptions.EntityNotFoundException:
            try:
                self.glue_client.create_connection(ConnectionInput=connection_input)
                log.info(f"Created Glue connection '{GLUE_DYNAMODB_CONNECTION_NAME}' for DynamoDB DataFrame source.")
            except Exception as e:
                log.error(f"Failed to create Glue connection '{GLUE_DYNAMODB_CONNECTION_NAME}': {e}")
                exit(1)
