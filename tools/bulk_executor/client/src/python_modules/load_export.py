from python_modules.shared.export.export_args import parse_export_args

help_text = f"""
    Purpose of "load-export":
        Loads a full export from S3 to an existing DynamoDB table.
        Required --table parameter to specify the name of destination DynamoDB table.
        Required --s3-path to specify the name of the S3 path where the export resides.
        Optional --transform parameter to specify the transform module containing transform_full_record and/or transform_incremental_record functions.

    Examples:
        Assuming you have your exports in s3://exported-data/prod/AWSDynamoDB/01716790307109-5f9d6aaa
        bulk load-export --table users --s3-path s3://bucket/prefix/AWSDynamoDB/01716790307109-5f9d6aaa [--transform example]
    """


def run(env_configs):
    return parse_export_args('load-export', help_text, env_configs)
