import mysql.connector
import decimal
import datetime
import boto3

sql = "SELECT * FROM customer_features.v_features WHERE record_type = 'events'"
s3_bucket = 's3-import-demo'
s3_path = 'demo/'
region = 'us-east-2'
items_per_file = 5


def main():
    mydb = mysql.connector.connect(
      host="my-endpoint-host.us-east-1.rds.amazonaws.com",
      user="admin",
      password="mriA6p5M7eH"
    )

    cur = mydb.cursor(buffered=True, dictionary=True)

    cur.execute(sql)

    res = cur.fetchall()
    rowcount = 0
    filetext = ''
    for row in res:
        if rowcount % items_per_file == 0 and rowcount > 0:
            write_s3(s3_bucket, s3_path, f'data_upto_{rowcount}.json', filetext)
            filetext = ''
        rowcount += 1
        rowtext = '{"Item":{'
        for key in row:
            if row[key] is not None:
                rowtext += parse_attr(key, row[key]) + ','
        rowtext = rowtext[:-1] + '}}'

        filetext += rowtext + '\n'

    write_s3(s3_bucket, s3_path, f'data_upto_{rowcount}.json', filetext)


def write_s3(bucket, path, objname, obj):
    client = boto3.client('s3', region_name=region)
    fullpath = path + objname
    res = client.put_object(
        Body=obj,
        Bucket=bucket,
        Key=fullpath,
        ACL='public-read')

    print(f'HTTP {res["ResponseMetadata"]["HTTPStatusCode"]} for S3 object s3://{bucket}/{path}{objname}')

    return 'ok'


def parse_attr(key, value):
    rtype = 'S'
    rvalue = ''
    if isinstance(value, int):
        rvalue = str(value)
        rtype = 'N'

    elif isinstance(value, decimal.Decimal):
        rvalue = str(value)
        rtype = 'N'

    elif isinstance(value, datetime.datetime):
        rvalue = str(value)
        rtype = 'S'

    else:
        rvalue = value
        rtype = 'S'

    return '"' + key + '":{"' + rtype + '":"' + rvalue + '"}'


if __name__ == "__main__":
    main()
