import boto3
import botocore

# project files
from utils.logger import log


class EnvConfigs:
    def __init__(self, args):
        self.aws_region = self._get_aws_region(args)

        sts_client = boto3.client('sts', region_name=self.aws_region)

        try:
            self.aws_account_id = self._get_aws_account_id(sts_client)
            self.aws_session_role = self._get_aws_session_role(sts_client)
        except botocore.exceptions.NoCredentialsError as e:
            print('Unable to locate credentials. You can configure credentials by running "aws configure".') # Print Message Intentional to align w/ standard SDK messaging
            error_message = str(e)
            log.debug(error_message)
            exit(1)

        print(f"""
    Bulk Executor environment configurations:

      AWS -
        Account: {self.aws_account_id}
        Region: {self.aws_region}
        Role: {self.aws_session_role}
    """)

    def _get_aws_account_id(self, sts_client):
        try:
            caller_identity = sts_client.get_caller_identity()
            return caller_identity['Account']
        except botocore.exceptions.ClientError as e:
            error_message = str(e)
            log.debug(error_message)
            print('Unable to locate AWS Account ID. You can configure credentials by running "aws configure".') # Print Message Intentional to align w/ standard SDK messaging
            exit(1)

    def _get_aws_session_role(self, sts_client):
        try:
            caller_identity = sts_client.get_caller_identity()
            return caller_identity['Arn']
        except botocore.exceptions.ClientError as e:
            error_message = str(e)
            log.debug(error_message)
            print('Unable to locate AWS Session Role. You can configure credentials by running "aws configure".') # Print Message Intentional to align w/ standard SDK messaging
            exit(1)

    def _get_aws_region(self, args):
        try:
            default_region = boto3.Session().region_name
            region = args.get('XRegion') or default_region
            if not region:
                log.debug("AWS region not configured.  Set AWS_DEFAULT_REGION, --XRegion, or ~/.aws/config")
                print('Unable to locate AWS Region. You can configure credentials by running "aws configure".') # Print Message Intentional to align w/ standard SDK messaging
                exit(1)
            return region
        except botocore.exceptions.ClientError as e:
            error_message = str(e)
            log.debug(error_message)
            print('Unable to locate AWS Region. You can configure credentials by running "aws configure".') # Print Message Intentional to align w/ standard SDK messaging
            exit(1)
