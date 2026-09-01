"""Unit tests for get_error_message's Java/Py4J unwrapping in shared/errors.py.

A denied connector read reaches the driver as a Py4JJavaError whose str() is the whole
Java stack -- hundreds of frames -- with the sentence the user needs on the second line.
Before #332 nothing extracted it: the SDK v1 pattern in this function stopped matching
when Glue moved to the v2 SDK (`software.amazon.awssdk...DynamoDbException` rather than
`com.amazonaws...AmazonDynamoDBException`), so the "message" was the stack itself.

shared/errors.py is replaced by a Mock for the whole server suite, so these tests load it
from disk. That is deliberate: the extraction is the behaviour under test.
"""

import importlib

import pytest


@pytest.fixture
def errors():
    """Import the real module under its own name, with conftest's Mock put back after.

    Loading it from a file path under a different module name also works, but coverage
    does not credit the execution, so the extraction below would read as untested and
    invite someone to delete it."""
    import sys

    saved = {name: sys.modules[name] for name in
             ('python_modules.shared.errors', 'shared.errors') if name in sys.modules}
    for name in saved:
        del sys.modules[name]
    try:
        yield importlib.import_module('python_modules.shared.errors')
    finally:
        for name in list(sys.modules):
            if name in ('python_modules.shared.errors', 'shared.errors'):
                del sys.modules[name]
        sys.modules.update(saved)


DENIAL_V2 = """An error occurred while calling o304.load.
: software.amazon.awssdk.services.dynamodb.model.DynamoDbException: User: \
arn:aws:sts::1:assumed-role/R/GlueJobRunnerSession is not authorized to perform: \
dynamodb:Scan on resource: arn:aws:dynamodb:us-east-1:1:table/t (Service: DynamoDb, Status Code: 400)
\tat software.amazon.awssdk.services.dynamodb.model.DynamoDbException$BuilderImpl.build(DynamoDbException.java:113)
\tat org.apache.spark.sql.execution.datasources.v2.DataSourceV2Utils$.loadV2Source(DataSourceV2Utils.scala:157)
"""

WRAPPED_IN_SPARK = """An error occurred while calling o92.save.
: org.apache.spark.SparkException: Job aborted due to stage failure: Task 3 in stage 2.0 failed 4 times
\tat org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:2905)
Caused by: software.amazon.awssdk.services.dynamodb.model.DynamoDbException: User: R is not \
authorized to perform: dynamodb:BatchWriteItem on resource: table/t
\tat software.amazon.awssdk.core.internal.http.pipeline.stages.RetryableStage.execute(RetryableStage.java:86)
"""

MULTILINE_DETAIL = """An error occurred while calling o1.load.
: java.lang.IllegalArgumentException: Unsupported option 'dynamodb.throughput.read.percent'
    supplied for this connector version; use dynamodb.throughput.read instead
\tat com.amazonaws.services.glue.connectors.DynamoDbOptions.validate(DynamoDbOptions.scala:88)
"""


class TestJavaErrorUnwrapping:

    def test_sdk_v2_denial_yields_aws_sentence(self, errors):
        message = errors.get_error_message(Exception(DENIAL_V2))
        assert message.startswith('User: arn:aws:sts::1:assumed-role/R/GlueJobRunnerSession')
        assert 'dynamodb:Scan' in message
        assert 'at software.amazon' not in message and 'at org.apache.spark' not in message
        assert 'o304.load' not in message, "Py4J's wrapper line is not the error"

    def test_innermost_cause_wins_over_spark_boilerplate(self, errors):
        """The outer layer says "Job aborted due to stage failure"; the cause says why."""
        message = errors.get_error_message(Exception(WRAPPED_IN_SPARK))
        assert 'not authorized to perform: dynamodb:BatchWriteItem' in message
        assert 'Job aborted due to stage failure' not in message

    def test_message_continuation_lines_are_joined(self, errors):
        message = errors.get_error_message(Exception(MULTILINE_DETAIL))
        assert message == (
            "Unsupported option 'dynamodb.throughput.read.percent' supplied for this "
            "connector version; use dynamodb.throughput.read instead"
        )

    def test_sdk_v1_pattern_still_works(self, errors):
        """Glue 4 and earlier; kept so an older runtime does not regress."""
        v1 = ("com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException: "
              "Requested resource not found (Service: AmazonDynamoDBv2; Status Code: 400)")
        assert errors.get_error_message(Exception(v1)) == "Requested resource not found"

    def test_spark_parse_exception_splits_off_the_sql(self, errors):
        """Spark's ParseException carries the offending SQL after a marker line; the
        function pulls it onto one line. Pre-existing behaviour, untested until this
        file had a way to import the module."""
        class ParseError(Exception):
            desc = "Syntax error at or near 'FRM'\n== SQL ==\nSELECT * FRM t\n       ^^^"

        assert errors.get_error_message(ParseError()) == (
            "Syntax error at or near 'FRM' | SQL: SELECT * FRM t | ^^^"
        )

    def test_parse_exception_without_a_sql_section(self, errors):
        class ParseError(Exception):
            desc = "  Syntax error, nothing more  "

        assert errors.get_error_message(ParseError()) == "Syntax error, nothing more"

    def test_exception_with_a_message_attribute(self, errors):
        """Some Py4J/Java wrappers expose .message rather than a useful str()."""
        class WithMessage(Exception):
            message = "  the useful part  "

            def __str__(self):
                return "an unhelpful repr"

        assert errors.get_error_message(WithMessage()) == "the useful part"

    def test_a_plain_exception_is_unchanged(self, errors):
        assert errors.get_error_message(ValueError("just a message")) == "just a message"

    def test_boto_error_response_still_preferred(self, errors):
        """An AWS SDK error carries its own message; do not go looking in the string."""
        class Fake(Exception):
            response = {'Error': {'Code': 'AccessDeniedException', 'Message': 'denied by policy'}}

        assert errors.get_error_message(Fake()) == 'denied by policy'
