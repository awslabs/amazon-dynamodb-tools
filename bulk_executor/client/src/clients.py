import boto3


class Clients:
    def __init__(self, region):
        self.iam_client = boto3.client('iam')
        self.dynamodb_client = boto3.client('dynamodb', region_name=region)
        self.glue_client = boto3.client('glue', region_name=region)
        self.logs_client = boto3.client('logs', region_name=region)
        self.s3_client = boto3.client('s3', region_name=region)
        self.service_quotas_client = boto3.client('service-quotas', region_name=region)
