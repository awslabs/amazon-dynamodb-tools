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

    # Look for DynamoDB exception message
    dynamo_match = re.search(
        r'com\.amazonaws\.services\.dynamodbv2\.model\.AmazonDynamoDBException:\s*(.*?)\s*\(Service:',
        msg
    )
    if dynamo_match:
        return dynamo_match.group(1).strip()

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
