import json
import re

from pyspark.accumulators import AccumulatorParam


class ListAccumulator(AccumulatorParam):
    def zero(self, initialValue):
        return []

    def addInPlace(self, v1, v2):
        v1.extend(v2)  # or v1 + v2 for new list
        return v1


def get_error_code(e):
    error_response = None
    if hasattr(e, 'response') and e.response:
        error_response = e.response.get('Error')
    if error_response:
        error_code = error_response.get('Code')
        return error_code
    return None

def get_error_message(e):
    # First try AWS error format
    if hasattr(e, 'response') and e.response:
        error_response = e.response.get('Error')
        if error_response:
            msg = error_response.get('Message')
            if msg:
                return msg

    # JSONDecodeError handling
    if isinstance(e, json.JSONDecodeError):
        return f"Invalid JSON: {str(e)} | Parsed string: '{e.doc}'"

    # Attempt to parse embedded Glue JSON
    msg = str(e)
    json_match = re.search(r'\{.*\}', msg, re.DOTALL)
    if json_match:
        try:
            error_json = json.loads(json_match.group(0))
            failure_reason = error_json.get("Failure Reason")
            if failure_reason:
                return failure_reason
        except json.JSONDecodeError:
            pass  # fallback

    # Look for DynamoDB exception message (AWS SDK v1, which Glue 4 and earlier used)
    dynamo_match = re.search(
        r'com\.amazonaws\.services\.dynamodbv2\.model\.AmazonDynamoDBException:\s*(.*?)\s*\(Service:',
        msg
    )
    if dynamo_match:
        return dynamo_match.group(1).strip()

    # A Java exception relayed through Py4J. str() on one of these is the entire Java
    # stack -- hundreds of frames -- with the sentence that matters on the second line:
    #
    #   py4j.protocol.Py4JJavaError: An error occurred while calling o304.load.
    #   : software.amazon.awssdk.services.dynamodb.model.DynamoDbException: User: ...
    #           is not authorized to perform: dynamodb:Scan on resource: ...
    #           at software.amazon.awssdk...(DynamoDbException.java:113)
    #
    # Take the innermost cause's message: for a wrapped failure the outer layer is
    # usually Spark boilerplate ("Job aborted due to stage failure") and the cause is
    # the AWS sentence the user needs. The message can wrap onto continuation lines, so
    # keep going until a stack frame or a new exception header.
    java_causes = re.findall(
        r'^(?:: |Caused by: )(?:[\w$]+\.)+([\w$]*(?:Exception|Error)): '
        r'(.*(?:\n(?!\s*(?:at |\.\.\. )|: |Caused by: ).*)*)',
        msg, re.MULTILINE)
    if java_causes:
        _cls, detail = java_causes[-1]
        detail = ' '.join(detail.split())
        if detail:
            return detail

    # ParseException handling
    if hasattr(e, 'desc'):  # ParseException
        msg = e.desc
        parts = msg.split('== SQL ==')
        if len(parts) == 2:
            error_part = parts[0].strip()
            sql_part = ' | '.join(line.strip() for line in parts[1].split('\n') if line.strip())
            return f"{error_part} | SQL: {sql_part}"
        return msg.strip()
    if hasattr(e, 'message'):
        msg = e.message.strip()

    # Clean up any multiline messages
    return ' | '.join(line.strip() for line in msg.split('\n') if line.strip())
