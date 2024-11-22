import json
import boto3
import os
import hashlib
from datetime import datetime
from botocore.exceptions import ClientError

sqs = boto3.client('sqs')

QUEUE_URL = os.environ['SQS_FIFO_QUEUE_URL']
DLQ_URL = os.environ['SQS_DLQ_URL']

MAX_RETRIES = 3
MAX_DLQ_RETRIES = 3

def handler(event, context):
    messages_to_send = []

    for record in event['Records']:
        if record['eventName'] in ['INSERT', 'MODIFY', 'DELETE']:
            item = {k: list(v.values())[0] for k, v in record['dynamodb'].get('NewImage' if record['eventName'] != 'DELETE' else 'OldImage', {}).items()}
            timestamp = record['dynamodb']['ApproximateCreationDateTime']

            message_id = generate_unique_id(item, timestamp)

            message = {
                'id': message_id,
                'data': item,
                'event_type': record['eventName'],
                'timestamp': datetime.fromtimestamp(timestamp).isoformat()
            }

            messages_to_send.append({
                'Id': message_id,
                'MessageBody': json.dumps(message),
                'MessageDeduplicationId': message_id,
                'MessageGroupId': item.get('pk', 'default')
            })

    if messages_to_send:
        send_messages_with_retry(messages_to_send)

def generate_unique_id(item, timestamp):
    unique_string = json.dumps(item, sort_keys=True) + str(timestamp)
    return hashlib.md5(unique_string.encode()).hexdigest()

def send_messages_with_retry(messages):
    batch_size = 10
    for i in range(0, len(messages), batch_size):
        batch = messages[i:i+batch_size]
        retry_count = 0
        while retry_count < MAX_RETRIES:
            try:
                response = sqs.send_message_batch(
                    QueueUrl=QUEUE_URL,
                    Entries=batch
                )
                if 'Failed' in response and response['Failed']:
                    failed_messages = [msg for msg in batch if msg['Id'] in [fail['Id'] for fail in response['Failed']]]
                    batch = failed_messages
                    retry_count += 1
                    print(f"Retry {retry_count} for {len(failed_messages)} failed messages")
                else:
                    print(f"Successfully sent {len(response['Successful'])} messages to FIFO queue")
                    break
            except ClientError as e:
                print(f"ClientError on attempt {retry_count + 1}: {str(e)}")
                retry_count += 1

        # If still failed after all retries, send to DLQ
        if batch:
            send_to_dlq_with_retry(batch)

def send_to_dlq_with_retry(messages):
    for message in messages:
        retry_count = 0
        while retry_count < MAX_DLQ_RETRIES:
            try:
                sqs.send_message(
                    QueueUrl=DLQ_URL,
                    MessageBody=json.dumps({
                        'original_message': message,
                        'error': 'Failed to send to FIFO queue after multiple retries'
                    })
                )
                print(f"Message {message['Id']} sent to DLQ")
                break
            except Exception as e:
                retry_count += 1
                print(f"Attempt {retry_count} failed to send message {message['Id']} to DLQ: {str(e)}")

        if retry_count == MAX_DLQ_RETRIES:
            print(f"CRITICAL: Failed to send message {message['Id']} to DLQ after {MAX_DLQ_RETRIES} attempts")
            # It is clear you are having a bad day if you get this far!!
            # At this point, you might want to implement additional error handling
            # such as writing to a persistent store or triggering an alarm