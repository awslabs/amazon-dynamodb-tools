from python_modules.shared.export.export_args import parse_export_args

help_text = f"""
    Purpose of "revert-export":
        Reverts an incremental export by undoing all writes from that export window.
        Only works with incremental exports that have NEW_AND_OLD_IMAGES view type.
        Required --table parameter to specify the name of destination DynamoDB table.
        Required --s3-path to specify the name of the S3 path where the incremental export resides.
        Optional --transform parameter to filter which records to revert (applied before the revert logic).

    Examples:
        Assuming you have your incremental exports in s3://exported-data/prod/AWSDynamoDB/01716790307109-5f9d6aaa
        bulk revert-export --table users --s3-path s3://bucket/prefix/AWSDynamoDB/01716790307109-5f9d6aaa
        bulk revert-export --table users --s3-path s3://bucket/prefix/AWSDynamoDB/01716790307109-5f9d6aaa --transform my_filter
    """

def run(env_configs):
    return parse_export_args('revert-export', help_text, env_configs)
