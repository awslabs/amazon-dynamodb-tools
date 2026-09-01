"""Unit tests for server/src/python_modules/shared/driver_errors.py.

Covers surface(): an understood failure exits with one sentence and prints nothing else;
an unexpected one prints a banner plus its traceback and still exits with a one-line
reason; the reason is collapsed to a single line and bounded.

The contract these protect is what #332 is about. A driver-side failure used to be
re-raised, which handed the user three copies of the same problem -- Glue's
exception-analysis blob, Py4J's restatement, and the Python traceback -- with AWS's
actual sentence buried in a Java stack. Now the driver decides, the same way it does for
failures a worker recorded.
"""

import pytest

from python_modules.shared import driver_errors, worker_errors
from python_modules.shared.bulk_executor_error import BulkExecutorError
from python_modules.shared.driver_errors import UNEXPECTED_FAILURE_BANNER, surface

PY4J_DENIAL = """An error occurred while calling o304.load.
: software.amazon.awssdk.services.dynamodb.model.DynamoDbException: User: \
arn:aws:sts::1:assumed-role/Role/GlueJobRunnerSession is not authorized to perform: \
dynamodb:Scan on resource: arn:aws:dynamodb:us-east-1:1:table/t because no \
identity-based policy allows the dynamodb:Scan action (Service: DynamoDb, Status Code: 400)
\tat software.amazon.awssdk.services.dynamodb.model.DynamoDbException$BuilderImpl.build(DynamoDbException.java:113)
\tat software.amazon.awssdk.core.internal.http.pipeline.stages.RetryableStage.execute(RetryableStage.java:86)
"""


def _real_errors_module():
    """Load shared/errors.py from disk. conftest replaces that module with a Mock for the
    whole server suite, and str(exception) is not a substitute: the entire point here is
    the extraction get_error_message does on a Py4J stack."""
    import importlib.util
    from pathlib import Path

    path = Path(__file__).resolve().parents[3] / "server/src/python_modules/shared/errors.py"
    spec = importlib.util.spec_from_file_location("_real_errors_for_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(autouse=True)
def real_message_extraction(monkeypatch):
    """The classifier binds these at import time, against conftest's Mock."""
    real = _real_errors_module()
    monkeypatch.setattr(worker_errors, 'get_error_code', real.get_error_code)
    monkeypatch.setattr(worker_errors, 'get_error_message', real.get_error_message)


class TestSurfaceUnderstood:

    def test_bulk_executor_error_exits_with_its_sentence(self, capsys):
        with pytest.raises(SystemExit) as raised:
            surface(BulkExecutorError("Invalid 'where': no such column"))

        assert str(raised.value) == "Invalid 'where': no such column"
        assert capsys.readouterr().out == '', "nothing to add; the sentence is the report"

    def test_py4j_denial_exits_with_aws_own_sentence(self, capsys):
        """The row that made #332 worth filing: 300 lines of Java, one useful sentence."""
        with pytest.raises(SystemExit) as raised:
            surface(Exception(PY4J_DENIAL))

        reason = str(raised.value)
        assert 'is not authorized to perform: dynamodb:Scan' in reason
        assert 'at software.amazon' not in reason, "no Java frames in the closing line"
        assert 'o304.load' not in reason, "Py4J's own wrapper text is not the problem"
        assert capsys.readouterr().out == '', "a denial needs no traceback"

    def test_reason_is_one_line(self):
        with pytest.raises(SystemExit) as raised:
            surface(BulkExecutorError("first line\nsecond line\n\tindented third"))

        assert str(raised.value) == "first line second line indented third"

    def test_long_reason_is_bounded(self):
        with pytest.raises(SystemExit) as raised:
            surface(BulkExecutorError("x" * 5000))

        reason = str(raised.value)
        assert len(reason) == driver_errors.MAX_REASON_CHARS
        assert reason.endswith('...')


class TestSurfaceUnexpected:

    def test_prints_banner_and_traceback_then_exits_one_line(self, capsys):
        try:
            raise KeyError('pk')
        except KeyError as e:
            with pytest.raises(SystemExit) as raised:
                surface(e)

        out = capsys.readouterr().out
        assert UNEXPECTED_FAILURE_BANNER in out
        assert 'Traceback' in out and "raise KeyError('pk')" in out, \
            "the frames are the report for something we did not expect"

        reason = str(raised.value)
        assert reason == "KeyError: 'pk'", "the type, since a KeyError message is bare"
        assert 'Traceback' not in reason, \
            "the reason becomes Glue's ErrorMessage and the client's closing line"

    def test_exits_rather_than_re_raising(self):
        """Re-raising is what produced the Glue blob and the Py4J restatement."""
        try:
            raise RuntimeError('boom')
        except RuntimeError as e:
            with pytest.raises(SystemExit):
                surface(e)
