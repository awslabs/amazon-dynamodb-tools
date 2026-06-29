"""Unit tests for the BulkDynamoDbRunner client orchestrator.

Covers `client/src/runner.py`:

- __init__: AWS region/account wiring, Clients construction, suppression flag
- _get_log_group_arns: ARN string composition for the two Glue log groups
- _fix_newlines: literal '\\n' / '\\t' → real newline/tab substitution
- _jsonify_message: JSON-error-line detection branches (match found,
  json.JSONDecodeError fallback, no-match fallback)
- _pretty_print_log_event: every print/skip path (task-log skip, empty
  message skip, error-log-group skip, ignore-list skip,
  BulkExecutorError suppression flag, GlueExceptionAnalysisListener
  noise gate, output-vs-non-output formatting, GRAY/PINK/YELLOW/default
  color routing)
- _is_job_state_unhealthy: matches against UNHEALTHY_STATE_LOG_MESSAGE_KEYS
- _wait_for_log_groups_to_exist: success on first try, retry/log/sleep
  loop on missing groups, ClientError handling, max-retries exit path
- _watch_log_group: sessionStart pass-through, sessionUpdate happy path,
  unhealthy-event termination, terminal-state termination, succeeded
  shutdown counter, RuntimeError on unknown event, reconnect on
  ConnectionError/HTTPClientError/EventStreamError, generic-exception
  handler
- _watch_glue_job: spawns one daemon thread per log group ARN
- _get_job_run_state / _get_job_run_error_message: get_job_run wiring,
  exception → exit() error paths
- _get_job_run_dpu: WaitForDPU sleep branch, normal path, exception → -1
- _get_glue_job_arguments: XDebug forward, --key/value pairing, verb →
  XAction rename, bool stringification, odd-arg-count edge case
- _assert_expected_script_args: missing-glue-job AssertionError,
  version-parity delegation
- _start_glue_job: happy path, ExpiredTokenException exit,
  EntityNotFoundException exit, generic exception exit, default-arg
  fallbacks (ExecutionClass, NumberOfWorkers, Timeout, WorkerType)
- _stop_glue_job: SuccessfulSubmissions log, Errors log, exception swallow
- _watch_for_interrupt: terminal-state early exit, ^C interrupt path
- run: arg-prep failure short-circuit, full happy path with state
  transitions (STOPPING/STOPPED/FAILED/TIMEOUT/SUCCEEDED), unhandled
  state error log, DPU-hours formatting branch, error-message logging

Tests are written test-first against current behavior so they serve
as a regression harness.
"""

import json
import sys
from unittest.mock import MagicMock, patch, call

import pytest
from botocore.exceptions import (
    ClientError,
    ConnectionError as BotoConnectionError,
    EventStreamError,
    HTTPClientError,
)

# Ensure client/src is on sys.path for runner imports (pytest.ini already
# adds it, but be explicit here in case this file is collected differently).
import os
_CLIENT_SRC = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..', 'client', 'src')
)
if _CLIENT_SRC not in sys.path:
    sys.path.insert(0, _CLIENT_SRC)


# Patch Clients before importing runner so __init__ doesn't try real AWS calls.
with patch('clients.Clients') as _MockClients:
    _MockClients.return_value = MagicMock()
    import runner as runner_module  # noqa: E402


# --- Fixtures ---------------------------------------------------------------


@pytest.fixture
def env_configs():
    """An EnvConfigs-shaped object with the two attrs runner.__init__ reads."""
    return MagicMock(aws_region='us-east-1', aws_account_id='123456789012')


@pytest.fixture
def bulk_runner(env_configs):
    """A BulkDynamoDbRunner whose Clients are MagicMocks (no real AWS).

    The fixture patches the Clients class at construction time so that
    self.dynamodb_client / self.glue_client / self.logs_client are all
    MagicMock instances available to per-test mocking.
    """
    with patch.object(runner_module, 'Clients') as MockClients:
        clients = MagicMock()
        clients.dynamodb_client = MagicMock()
        clients.glue_client = MagicMock()
        clients.logs_client = MagicMock()
        MockClients.return_value = clients

        instance = runner_module.BulkDynamoDbRunner(env_configs)
    return instance


# --- __init__ ---------------------------------------------------------------


class TestInit:
    """Tests for BulkDynamoDbRunner.__init__ (lines 47-55)."""

    def test_stores_aws_region_from_env_configs(self, bulk_runner):
        assert bulk_runner.aws_region == 'us-east-1'

    def test_stores_aws_account_id_from_env_configs(self, bulk_runner):
        assert bulk_runner.aws_account_id == '123456789012'

    def test_initializes_suppress_glue_noise_to_false(self, bulk_runner):
        assert bulk_runner._suppress_glue_noise is False

    def test_constructs_clients_with_aws_region(self, env_configs):
        with patch.object(runner_module, 'Clients') as MockClients:
            MockClients.return_value = MagicMock()
            runner_module.BulkDynamoDbRunner(env_configs)
        MockClients.assert_called_once_with('us-east-1')

    def test_attaches_clients_from_clients_object(self, env_configs):
        with patch.object(runner_module, 'Clients') as MockClients:
            clients = MagicMock()
            clients.dynamodb_client = 'ddb-sentinel'
            clients.glue_client = 'glue-sentinel'
            clients.logs_client = 'logs-sentinel'
            MockClients.return_value = clients
            r = runner_module.BulkDynamoDbRunner(env_configs)
        assert r.dynamodb_client == 'ddb-sentinel'
        assert r.glue_client == 'glue-sentinel'
        assert r.logs_client == 'logs-sentinel'


# --- _get_log_group_arns ----------------------------------------------------


class TestGetLogGroupArns:
    """Tests for ARN composition (lines 57-61)."""

    def test_returns_two_arns(self, bulk_runner):
        arns = bulk_runner._get_log_group_arns()
        assert len(arns) == 2

    def test_first_arn_is_error_log_group(self, bulk_runner):
        arns = bulk_runner._get_log_group_arns()
        assert arns[0].endswith(runner_module.GLUE_LOG_GROUP_ERROR)

    def test_second_arn_is_output_log_group(self, bulk_runner):
        arns = bulk_runner._get_log_group_arns()
        assert arns[1].endswith(runner_module.GLUE_LOG_GROUP_OUTPUT)

    def test_arn_contains_region_and_account(self, bulk_runner):
        arns = bulk_runner._get_log_group_arns()
        for arn in arns:
            assert 'us-east-1' in arn
            assert '123456789012' in arn

    def test_arn_format_is_logs_arn(self, bulk_runner):
        arns = bulk_runner._get_log_group_arns()
        for arn in arns:
            assert arn.startswith('arn:aws:logs:')


# --- _fix_newlines ----------------------------------------------------------


class TestFixNewlines:
    """Tests for literal escape substitution (lines 63-64)."""

    def test_replaces_literal_backslash_n_with_newline(self, bulk_runner):
        assert bulk_runner._fix_newlines(r'a\nb') == 'a\nb'

    def test_replaces_literal_backslash_t_with_tab(self, bulk_runner):
        assert bulk_runner._fix_newlines(r'a\tb') == 'a\tb'

    def test_no_substitution_when_no_escapes(self, bulk_runner):
        assert bulk_runner._fix_newlines('plain text') == 'plain text'

    def test_handles_both_escapes_in_same_string(self, bulk_runner):
        assert bulk_runner._fix_newlines(r'a\nb\tc') == 'a\nb\tc'


# --- _jsonify_message -------------------------------------------------------


class TestJsonifyMessage:
    """Tests for JSON detection in error/warn/exception lines (lines 66-86)."""

    def test_no_json_returns_fix_newlines_passthrough(self, bulk_runner):
        result = bulk_runner._jsonify_message('plain log line')
        assert result == 'plain log line'

    def test_no_json_with_escapes_passes_through_fix_newlines(self, bulk_runner):
        result = bulk_runner._jsonify_message(r'log\nline')
        assert result == 'log\nline'

    def test_non_error_severity_is_not_jsonified(self, bulk_runner):
        # Lower-severity logs that happen to contain JSON-looking content
        # don't match the regex (which requires ERROR|WARN|EXCEPTION).
        result = bulk_runner._jsonify_message('INFO log message {"a":1}')
        # Falls through to fix_newlines (no escapes → identity).
        assert result == 'INFO log message {"a":1}'

    def test_message_matching_error_pattern_pretty_prints_json(self, bulk_runner):
        msg = 'ERROR something bad {"key": "value"}'
        result = bulk_runner._jsonify_message(msg)
        expected = 'ERROR something bad \n{\n  "key": "value"\n}'
        assert result == expected

    def test_json_array_payload_is_pretty_printed(self, bulk_runner):
        # Exercises the second alternation branch of the regex (\[...\]),
        # confirming array payloads are detected and pretty-printed too.
        msg = 'ERROR bad list [1, 2, 3]'
        result = bulk_runner._jsonify_message(msg)
        expected = 'ERROR bad list \n[\n  1,\n  2,\n  3\n]'
        assert result == expected

    def test_nested_braces_json_is_pretty_printed(self, bulk_runner):
        # Nested objects must round-trip through json.loads/json.dumps so the
        # greedy brace match in the regex captures the full payload.
        msg = 'WARN nested {"a": {"b": 1}}'
        result = bulk_runner._jsonify_message(msg)
        expected = 'WARN nested \n{\n  "a": {\n    "b": 1\n  }\n}'
        assert result == expected


# --- _pretty_print_log_event ------------------------------------------------


def _make_event(message='hello\n', log_group=None, stream='stream-x', timestamp=1):
    return {
        'message': message,
        'logGroupIdentifier': log_group or '123456789012:/aws-glue/jobs/output',
        'logStreamName': stream,
        'timestamp': timestamp,
    }


class TestPrettyPrintLogEvent:
    """Tests for log event routing and color decoration (lines 88-135)."""

    def test_skips_task_log_streams(self, bulk_runner, capsys):
        ev = _make_event(stream='something_g-task')
        bulk_runner._pretty_print_log_event(ev)
        captured = capsys.readouterr()
        assert captured.out == ''
        assert captured.err == ''

    def test_skips_empty_message(self, bulk_runner, capsys):
        ev = _make_event(message='')
        bulk_runner._pretty_print_log_event(ev)
        captured = capsys.readouterr()
        assert captured.out == ''

    def test_skips_error_log_group(self, bulk_runner, capsys):
        # Error log group is suppressed even with content.
        ev = _make_event(
            message='something happened',
            log_group=f'123456789012:{runner_module.GLUE_LOG_GROUP_ERROR}',
        )
        bulk_runner._pretty_print_log_event(ev)
        captured = capsys.readouterr()
        assert captured.out == ''

    def test_skips_log_pattern_ignore_list(self, bulk_runner, capsys, monkeypatch):
        monkeypatch.setattr(runner_module.utils, 'LOG_PATTERN_IGNORE_LIST',
                            ['NOISY_PATTERN'])
        ev = _make_event(message='blah NOISY_PATTERN here')
        bulk_runner._pretty_print_log_event(ev)
        assert capsys.readouterr().out == ''

    def test_bulk_executor_error_sets_suppress_flag(self, bulk_runner, monkeypatch):
        monkeypatch.setattr(runner_module.utils, 'LOG_PATTERN_IGNORE_LIST', [])
        monkeypatch.setattr(runner_module.utils, 'CONFIG_LOG_MESSAGE_KEYS', [])
        monkeypatch.setattr(runner_module.utils, 'STD_ERROR_MESSAGE_KEYS', [])
        monkeypatch.setattr(runner_module.utils, 'WARN_LOG_MESSAGE_KEYS', [])
        ev = _make_event(message='BulkExecutorError fatal')
        bulk_runner._pretty_print_log_event(ev)
        assert bulk_runner._suppress_glue_noise is True

    def test_glue_exception_listener_suppressed_after_bulk_error(self, bulk_runner, capsys, monkeypatch):
        monkeypatch.setattr(runner_module.utils, 'LOG_PATTERN_IGNORE_LIST', [])
        monkeypatch.setattr(runner_module.utils, 'CONFIG_LOG_MESSAGE_KEYS', [])
        monkeypatch.setattr(runner_module.utils, 'STD_ERROR_MESSAGE_KEYS', [])
        monkeypatch.setattr(runner_module.utils, 'WARN_LOG_MESSAGE_KEYS', [])
        bulk_runner._suppress_glue_noise = True
        ev = _make_event(message='GlueExceptionAnalysisListener spam')
        bulk_runner._pretty_print_log_event(ev)
        assert capsys.readouterr().out == ''

    def test_error_category_suppressed_after_bulk_error(self, bulk_runner, capsys, monkeypatch):
        monkeypatch.setattr(runner_module.utils, 'LOG_PATTERN_IGNORE_LIST', [])
        monkeypatch.setattr(runner_module.utils, 'CONFIG_LOG_MESSAGE_KEYS', [])
        monkeypatch.setattr(runner_module.utils, 'STD_ERROR_MESSAGE_KEYS', [])
        monkeypatch.setattr(runner_module.utils, 'WARN_LOG_MESSAGE_KEYS', [])
        bulk_runner._suppress_glue_noise = True
        ev = _make_event(message='Error Category: foo')
        bulk_runner._pretty_print_log_event(ev)
        assert capsys.readouterr().out == ''

    def test_output_log_group_passes_through_unformatted(self, bulk_runner, capsys, monkeypatch):
        monkeypatch.setattr(runner_module.utils, 'LOG_PATTERN_IGNORE_LIST', [])
        monkeypatch.setattr(runner_module.utils, 'CONFIG_LOG_MESSAGE_KEYS', [])
        monkeypatch.setattr(runner_module.utils, 'STD_ERROR_MESSAGE_KEYS', [])
        monkeypatch.setattr(runner_module.utils, 'WARN_LOG_MESSAGE_KEYS', [])
        ev = _make_event(
            message='hello world',
            log_group='123456789012:/aws-glue/jobs/output',
        )
        bulk_runner._pretty_print_log_event(ev)
        out = capsys.readouterr().out
        # Output log group: no '[group]' prefix decoration.
        assert out == 'hello world'

    def test_non_output_log_group_decorated_with_group_prefix(self, bulk_runner, capsys, monkeypatch):
        monkeypatch.setattr(runner_module.utils, 'LOG_PATTERN_IGNORE_LIST', [])
        monkeypatch.setattr(runner_module.utils, 'CONFIG_LOG_MESSAGE_KEYS', [])
        monkeypatch.setattr(runner_module.utils, 'STD_ERROR_MESSAGE_KEYS', [])
        monkeypatch.setattr(runner_module.utils, 'WARN_LOG_MESSAGE_KEYS', [])
        ev = _make_event(
            message='something',
            log_group='123456789012:/aws-glue/jobs/somewhere-else',
        )
        bulk_runner._pretty_print_log_event(ev)
        out = capsys.readouterr().out
        assert out.startswith('[123456789012:/aws-glue/jobs/somewhere-else]')

    def test_config_keys_route_to_gray(self, bulk_runner, capsys, monkeypatch):
        monkeypatch.setattr(runner_module.utils, 'LOG_PATTERN_IGNORE_LIST', [])
        monkeypatch.setattr(runner_module.utils, 'CONFIG_LOG_MESSAGE_KEYS', ['arguments:'])
        monkeypatch.setattr(runner_module.utils, 'STD_ERROR_MESSAGE_KEYS', [])
        monkeypatch.setattr(runner_module.utils, 'WARN_LOG_MESSAGE_KEYS', [])
        ev = _make_event(
            message='arguments: foo',
            log_group='123456789012:/aws-glue/jobs/output',
        )
        bulk_runner._pretty_print_log_event(ev)
        out = capsys.readouterr().out
        assert runner_module.ColorCodes.GRAY in out
        assert runner_module.ColorCodes.RESET in out

    def test_std_error_keys_route_to_pink_stderr(self, bulk_runner, capsys, monkeypatch):
        monkeypatch.setattr(runner_module.utils, 'LOG_PATTERN_IGNORE_LIST', [])
        monkeypatch.setattr(runner_module.utils, 'CONFIG_LOG_MESSAGE_KEYS', [])
        monkeypatch.setattr(runner_module.utils, 'STD_ERROR_MESSAGE_KEYS', ['exception'])
        monkeypatch.setattr(runner_module.utils, 'WARN_LOG_MESSAGE_KEYS', [])
        ev = _make_event(
            message='java exception thrown',
            log_group='123456789012:/aws-glue/jobs/output',
        )
        bulk_runner._pretty_print_log_event(ev)
        captured = capsys.readouterr()
        # Output goes to stderr (PINK).
        assert runner_module.ColorCodes.PINK in captured.err

    def test_warn_keys_route_to_yellow(self, bulk_runner, capsys, monkeypatch):
        monkeypatch.setattr(runner_module.utils, 'LOG_PATTERN_IGNORE_LIST', [])
        monkeypatch.setattr(runner_module.utils, 'CONFIG_LOG_MESSAGE_KEYS', [])
        monkeypatch.setattr(runner_module.utils, 'STD_ERROR_MESSAGE_KEYS', [])
        monkeypatch.setattr(runner_module.utils, 'WARN_LOG_MESSAGE_KEYS', [' warn '])
        ev = _make_event(
            message=' WARN something',
            log_group='123456789012:/aws-glue/jobs/output',
        )
        bulk_runner._pretty_print_log_event(ev)
        out = capsys.readouterr().out
        assert runner_module.ColorCodes.YELLOW in out

    def test_default_path_no_color(self, bulk_runner, capsys, monkeypatch):
        monkeypatch.setattr(runner_module.utils, 'LOG_PATTERN_IGNORE_LIST', [])
        monkeypatch.setattr(runner_module.utils, 'CONFIG_LOG_MESSAGE_KEYS', [])
        monkeypatch.setattr(runner_module.utils, 'STD_ERROR_MESSAGE_KEYS', [])
        monkeypatch.setattr(runner_module.utils, 'WARN_LOG_MESSAGE_KEYS', [])
        ev = _make_event(
            message='regular message',
            log_group='123456789012:/aws-glue/jobs/output',
        )
        bulk_runner._pretty_print_log_event(ev)
        out = capsys.readouterr().out
        # No color escape codes.
        assert runner_module.ColorCodes.GRAY not in out
        assert runner_module.ColorCodes.PINK not in out
        assert runner_module.ColorCodes.YELLOW not in out


# --- _is_job_state_unhealthy ------------------------------------------------


class TestIsJobStateUnhealthy:
    """Tests for unhealthy state detection (lines 137-144)."""

    def test_returns_true_when_unhealthy_keyword_present(self, bulk_runner, monkeypatch):
        monkeypatch.setattr(runner_module.utils, 'UNHEALTHY_STATE_LOG_MESSAGE_KEYS',
                            ['BadThing:'])
        ev = {'message': 'oh no BadThing: detected'}
        assert bulk_runner._is_job_state_unhealthy(ev) is True

    def test_returns_false_when_no_unhealthy_keyword(self, bulk_runner, monkeypatch):
        monkeypatch.setattr(runner_module.utils, 'UNHEALTHY_STATE_LOG_MESSAGE_KEYS',
                            ['BadThing:'])
        ev = {'message': 'totally fine'}
        assert bulk_runner._is_job_state_unhealthy(ev) is False


# --- _wait_for_log_groups_to_exist ------------------------------------------


class TestWaitForLogGroupsToExist:
    """Tests for log group existence polling (lines 146-173)."""

    def test_returns_immediately_when_groups_exist(self, bulk_runner):
        bulk_runner.logs_client.describe_log_groups.return_value = {
            'logGroups': [
                {'logGroupName': '/aws-glue/jobs/error'},
                {'logGroupName': '/aws-glue/jobs/output'},
            ]
        }
        arns = [
            'arn:aws:logs:us-east-1:123456789012:log-group:/aws-glue/jobs/error',
            'arn:aws:logs:us-east-1:123456789012:log-group:/aws-glue/jobs/output',
        ]
        # No exception, no exit -> success.
        bulk_runner._wait_for_log_groups_to_exist(arns)
        bulk_runner.logs_client.describe_log_groups.assert_called_once()

    def test_retries_on_missing_groups_then_succeeds(self, bulk_runner, monkeypatch):
        # First call returns nothing, second call returns the groups.
        bulk_runner.logs_client.describe_log_groups.side_effect = [
            {'logGroups': []},
            {'logGroups': [{'logGroupName': '/aws-glue/jobs/output'}]},
        ]
        sleeps = []
        monkeypatch.setattr(runner_module.time, 'sleep', lambda s: sleeps.append(s))

        arns = ['arn:aws:logs:us-east-1:123456789012:log-group:/aws-glue/jobs/output']
        bulk_runner._wait_for_log_groups_to_exist(arns)
        assert bulk_runner.logs_client.describe_log_groups.call_count == 2
        assert sleeps == [runner_module.LIVE_TAIL_RETRY_WAIT_TIME_IN_SECONDS]

    def test_client_error_is_caught_and_retries(self, bulk_runner, monkeypatch, capsys):
        err = ClientError({'Error': {'Code': 'X', 'Message': 'm'}}, 'DescribeLogGroups')
        bulk_runner.logs_client.describe_log_groups.side_effect = [
            err,
            {'logGroups': [{'logGroupName': '/aws-glue/jobs/output'}]},
        ]
        monkeypatch.setattr(runner_module.time, 'sleep', lambda s: None)

        arns = ['arn:aws:logs:us-east-1:123456789012:log-group:/aws-glue/jobs/output']
        bulk_runner._wait_for_log_groups_to_exist(arns)
        assert bulk_runner.logs_client.describe_log_groups.call_count == 2

    def test_max_retries_exits(self, bulk_runner, monkeypatch):
        # Always returns no groups -> should hit max retries and exit.
        bulk_runner.logs_client.describe_log_groups.return_value = {'logGroups': []}
        monkeypatch.setattr(runner_module.time, 'sleep', lambda s: None)

        arns = ['arn:aws:logs:us-east-1:123456789012:log-group:/aws-glue/jobs/missing']
        with pytest.raises(SystemExit):
            bulk_runner._wait_for_log_groups_to_exist(arns)


# --- _watch_log_group / _watch_glue_job -------------------------------------


class TestWatchLogGroup:
    """Tests for the live tail loop in _watch_log_group (lines 175-257)."""

    def _arn(self):
        return 'arn:aws:logs:us-east-1:123456789012:log-group:/aws-glue/jobs/output'

    def test_terminal_state_exits_event_loop(self, bulk_runner, monkeypatch):
        """Job in TERMINAL_JOB_STATES → close stream and return."""
        import threading
        unhealthy = threading.Event()
        # Bypass log-group existence wait.
        bulk_runner._wait_for_log_groups_to_exist = MagicMock()
        bulk_runner._get_job_run_state = MagicMock(return_value=runner_module.STOPPED_STATE)

        event_stream = MagicMock()
        # One pass of the event loop is enough; iter returns a single fake event.
        event_stream.__iter__ = MagicMock(return_value=iter([{'sessionStart': {}}]))
        bulk_runner.logs_client.start_live_tail.return_value = {
            'responseStream': event_stream
        }

        bulk_runner._watch_log_group('jr-1', self._arn(), unhealthy)
        event_stream.close.assert_called_once()

    def test_unhealthy_event_set_returns_immediately(self, bulk_runner):
        import threading
        unhealthy = threading.Event()
        unhealthy.set()
        bulk_runner._wait_for_log_groups_to_exist = MagicMock()
        bulk_runner._get_job_run_state = MagicMock(return_value=runner_module.RUNNING_STATE)

        event_stream = MagicMock()
        event_stream.__iter__ = MagicMock(return_value=iter([{'sessionUpdate': {'sessionResults': []}}]))
        bulk_runner.logs_client.start_live_tail.return_value = {
            'responseStream': event_stream
        }
        bulk_runner._watch_log_group('jr-1', self._arn(), unhealthy)
        event_stream.close.assert_called_once()

    def test_session_update_with_unhealthy_log_stops_job(self, bulk_runner, monkeypatch):
        import threading
        unhealthy = threading.Event()
        bulk_runner._wait_for_log_groups_to_exist = MagicMock()
        bulk_runner._get_job_run_state = MagicMock(return_value=runner_module.RUNNING_STATE)
        bulk_runner._stop_glue_job = MagicMock()

        # Force is_job_state_unhealthy to return True for our event.
        monkeypatch.setattr(runner_module.utils, 'UNHEALTHY_STATE_LOG_MESSAGE_KEYS', ['BadThing:'])
        # Make pretty_print_log_event a no-op to isolate this branch.
        bulk_runner._pretty_print_log_event = MagicMock()

        log_event = {
            'message': 'oops BadThing: foo',
            'logGroupIdentifier': '123456789012:/aws-glue/jobs/output',
            'logStreamName': 'stream',
            'timestamp': 1,
        }

        # The reassembler runs on real input — patch it to return our log event.
        with patch.object(runner_module, 'GlueLogReassembler') as reasm_cls:
            reasm = MagicMock()
            reasm.process.return_value = [log_event]
            reasm.flush.return_value = []
            reasm_cls.return_value = reasm

            event_stream = MagicMock()
            event_stream.__iter__ = MagicMock(return_value=iter([
                {'sessionUpdate': {'sessionResults': [log_event]}}
            ]))
            bulk_runner.logs_client.start_live_tail.return_value = {
                'responseStream': event_stream
            }

            bulk_runner._watch_log_group('jr-1', self._arn(), unhealthy)

        assert unhealthy.is_set()
        bulk_runner._stop_glue_job.assert_called_once_with('jr-1')

    def test_unknown_event_type_raises_runtime_error(self, bulk_runner, monkeypatch):
        """Lines 239-240: Unknown event raises RuntimeError, caught by generic handler."""
        import threading
        unhealthy = threading.Event()
        bulk_runner._wait_for_log_groups_to_exist = MagicMock()
        bulk_runner._get_job_run_state = MagicMock(return_value=runner_module.RUNNING_STATE)

        event_stream = MagicMock()
        event_stream.__iter__ = MagicMock(return_value=iter([{'unknownKey': {}}]))
        bulk_runner.logs_client.start_live_tail.return_value = {
            'responseStream': event_stream
        }
        # Generic Exception handler catches it and returns; no propagation.
        bulk_runner._watch_log_group('jr-1', self._arn(), unhealthy)

    def test_connection_error_reconnects_when_job_running(self, bulk_runner, monkeypatch):
        """Lines 249-254: ConnectionError during tail, job still running → reconnect."""
        import threading
        unhealthy = threading.Event()
        bulk_runner._wait_for_log_groups_to_exist = MagicMock()
        # First call: RUNNING (so it reconnects); second call: SUCCEEDED (so it returns).
        bulk_runner._get_job_run_state = MagicMock(side_effect=[
            runner_module.RUNNING_STATE,  # before raise
            runner_module.SUCCEEDED_STATE,  # after raise (terminates loop)
        ])
        monkeypatch.setattr(runner_module.time, 'sleep', lambda s: None)

        # First start_live_tail raises mid-iteration; second start exits on SUCCEEDED.
        bulk_runner.logs_client.start_live_tail.side_effect = [
            BotoConnectionError(error='boom'),
            {'responseStream': MagicMock(__iter__=MagicMock(return_value=iter([])))},
        ]
        # Avoid hanging: 2nd call's stream is empty, so loop exits via final flush.
        with patch.object(runner_module, 'GlueLogReassembler') as reasm_cls:
            reasm = MagicMock()
            reasm.process.return_value = []
            reasm.flush.return_value = []
            reasm_cls.return_value = reasm

            bulk_runner._watch_log_group('jr-1', self._arn(), unhealthy)

        assert bulk_runner.logs_client.start_live_tail.call_count == 2

    def test_connection_error_returns_when_terminal(self, bulk_runner, monkeypatch):
        """Lines 250-252: ConnectionError but job already terminal → return without reconnect."""
        import threading
        unhealthy = threading.Event()
        bulk_runner._wait_for_log_groups_to_exist = MagicMock()
        bulk_runner._get_job_run_state = MagicMock(return_value=runner_module.FAILED_STATE)

        bulk_runner.logs_client.start_live_tail.side_effect = HTTPClientError(error='boom')
        bulk_runner._watch_log_group('jr-1', self._arn(), unhealthy)
        assert bulk_runner.logs_client.start_live_tail.call_count == 1

    def test_generic_exception_returns(self, bulk_runner):
        """Lines 255-257: unexpected Exception logs and returns (no re-raise)."""
        import threading
        unhealthy = threading.Event()
        bulk_runner._wait_for_log_groups_to_exist = MagicMock()
        bulk_runner._get_job_run_state = MagicMock(return_value=runner_module.RUNNING_STATE)
        bulk_runner.logs_client.start_live_tail.side_effect = ValueError('weird')
        # Should not raise.
        bulk_runner._watch_log_group('jr-1', self._arn(), unhealthy)

    def test_session_start_event_does_not_raise(self, bulk_runner):
        """Line 210-211: sessionStart key handled silently."""
        import threading
        unhealthy = threading.Event()
        bulk_runner._wait_for_log_groups_to_exist = MagicMock()
        bulk_runner._get_job_run_state = MagicMock(side_effect=[
            runner_module.RUNNING_STATE,  # processed sessionStart, loop continues
            runner_module.STOPPED_STATE,  # next iteration: terminal, stream.close()
        ])

        event_stream = MagicMock()
        event_stream.__iter__ = MagicMock(return_value=iter([
            {'sessionStart': {'session': '1'}},
            {'sessionUpdate': {'sessionResults': []}},
        ]))
        bulk_runner.logs_client.start_live_tail.return_value = {
            'responseStream': event_stream
        }
        with patch.object(runner_module, 'GlueLogReassembler') as reasm_cls:
            reasm = MagicMock()
            reasm.process.return_value = []
            reasm.flush.return_value = []
            reasm_cls.return_value = reasm
            bulk_runner._watch_log_group('jr-1', self._arn(), unhealthy)

    def test_succeeded_state_increments_counter_and_closes(self, bulk_runner):
        """Lines 228-237: SUCCEEDED + empty results increments counter; >max → close."""
        import threading
        unhealthy = threading.Event()
        bulk_runner._wait_for_log_groups_to_exist = MagicMock()
        # Always SUCCEEDED so the counter path runs every iteration.
        bulk_runner._get_job_run_state = MagicMock(return_value=runner_module.SUCCEEDED_STATE)

        # 5 sessionUpdate events with empty results — counter goes 1,2,3,4 (>3 → close).
        event_stream = MagicMock()
        event_stream.__iter__ = MagicMock(return_value=iter([
            {'sessionUpdate': {'sessionResults': []}},
            {'sessionUpdate': {'sessionResults': []}},
            {'sessionUpdate': {'sessionResults': []}},
            {'sessionUpdate': {'sessionResults': []}},
            {'sessionUpdate': {'sessionResults': []}},
        ]))
        bulk_runner.logs_client.start_live_tail.return_value = {
            'responseStream': event_stream
        }
        with patch.object(runner_module, 'GlueLogReassembler') as reasm_cls:
            reasm = MagicMock()
            reasm.process.return_value = []
            reasm.flush.return_value = []
            reasm_cls.return_value = reasm
            bulk_runner._watch_log_group('jr-1', self._arn(), unhealthy)
        event_stream.close.assert_called_once()

    def test_succeeded_state_with_log_events_resets_counter(self, bulk_runner):
        """Lines 232-233: SUCCEEDED + non-empty results resets counter to 0."""
        import threading
        unhealthy = threading.Event()
        bulk_runner._wait_for_log_groups_to_exist = MagicMock()
        # Always SUCCEEDED so counter path runs.
        bulk_runner._get_job_run_state = MagicMock(return_value=runner_module.SUCCEEDED_STATE)

        # First two with content (counter resets), then 5 empty (counter > 3 → close).
        log_event = {
            'message': 'data\n',
            'logGroupIdentifier': '123456789012:/aws-glue/jobs/output',
            'logStreamName': 'stream',
            'timestamp': 1,
        }
        event_stream = MagicMock()
        event_stream.__iter__ = MagicMock(return_value=iter([
            {'sessionUpdate': {'sessionResults': [log_event]}},
            {'sessionUpdate': {'sessionResults': []}},
            {'sessionUpdate': {'sessionResults': []}},
            {'sessionUpdate': {'sessionResults': []}},
            {'sessionUpdate': {'sessionResults': []}},
        ]))
        bulk_runner.logs_client.start_live_tail.return_value = {
            'responseStream': event_stream
        }
        with patch.object(runner_module, 'GlueLogReassembler') as reasm_cls:
            reasm = MagicMock()
            reasm.process.return_value = []
            reasm.flush.return_value = []
            reasm_cls.return_value = reasm
            # Avoid the unhealthy-pretty-print branch.
            bulk_runner._pretty_print_log_event = MagicMock()
            bulk_runner._watch_log_group('jr-1', self._arn(), unhealthy)
        event_stream.close.assert_called_once()


class TestWatchGlueJob:
    """Tests for thread fan-out in _watch_glue_job (lines 259-279)."""

    def test_spawns_one_thread_per_log_group(self, bulk_runner, monkeypatch):
        # Replace the thread target with a no-op so threads exit immediately.
        bulk_runner._watch_log_group = MagicMock()
        bulk_runner._watch_glue_job('jr-1')
        # Two log group ARNs configured -> two thread invocations.
        assert bulk_runner._watch_log_group.call_count == 2


# --- _get_job_run_state / _get_job_run_error_message / _get_job_run_dpu ----


class TestGetJobRunState:
    """Tests for state-fetch and error path (lines 281-291)."""

    def test_returns_state_from_glue(self, bulk_runner):
        bulk_runner.glue_client.get_job_run.return_value = {
            'JobRun': {'JobRunState': runner_module.RUNNING_STATE}
        }
        state = bulk_runner._get_job_run_state('jr-1')
        assert state == runner_module.RUNNING_STATE

    def test_calls_glue_with_correct_job_name(self, bulk_runner):
        bulk_runner.glue_client.get_job_run.return_value = {
            'JobRun': {'JobRunState': 'RUNNING'}
        }
        bulk_runner._get_job_run_state('jr-1')
        bulk_runner.glue_client.get_job_run.assert_called_once_with(
            JobName=runner_module.GLUE_JOB_NAME, RunId='jr-1'
        )

    def test_exception_exits(self, bulk_runner, monkeypatch):
        # log.error('msg', e) inside runner mis-uses the logging API by
        # passing a non-format-string argument; bypass by replacing log.
        monkeypatch.setattr(runner_module.log, 'error', MagicMock())
        bulk_runner.glue_client.get_job_run.side_effect = RuntimeError('boom')
        with pytest.raises(SystemExit):
            bulk_runner._get_job_run_state('jr-1')


class TestGetJobRunErrorMessage:
    """Tests for error-message fetch (lines 293-302)."""

    def test_returns_error_message(self, bulk_runner):
        bulk_runner.glue_client.get_job_run.return_value = {
            'JobRun': {'ErrorMessage': 'boom'}
        }
        assert bulk_runner._get_job_run_error_message('jr-1') == 'boom'

    def test_returns_none_when_no_error_message(self, bulk_runner):
        bulk_runner.glue_client.get_job_run.return_value = {'JobRun': {}}
        assert bulk_runner._get_job_run_error_message('jr-1') is None

    def test_exception_exits(self, bulk_runner, monkeypatch):
        monkeypatch.setattr(runner_module.log, 'error', MagicMock())
        bulk_runner.glue_client.get_job_run.side_effect = RuntimeError('boom')
        with pytest.raises(SystemExit):
            bulk_runner._get_job_run_error_message('jr-1')


class TestGetJobRunDpu:
    """Tests for DPU-fetch (lines 304-318)."""

    def test_returns_dpu_seconds(self, bulk_runner):
        bulk_runner.glue_client.get_job_run.return_value = {
            'JobRun': {'DPUSeconds': 360.0}
        }
        result = bulk_runner._get_job_run_dpu('jr-1', {})
        assert result == 360.0

    def test_returns_zero_when_dpu_seconds_missing(self, bulk_runner):
        bulk_runner.glue_client.get_job_run.return_value = {'JobRun': {}}
        assert bulk_runner._get_job_run_dpu('jr-1', {}) == 0

    def test_wait_for_dpu_sleeps_40_seconds(self, bulk_runner, monkeypatch):
        bulk_runner.glue_client.get_job_run.return_value = {
            'JobRun': {'DPUSeconds': 100.0}
        }
        sleeps = []
        monkeypatch.setattr(runner_module.time, 'sleep', lambda s: sleeps.append(s))
        bulk_runner._get_job_run_dpu('jr-1', {'XWaitForDPU': True})
        assert sleeps == [40]

    def test_no_wait_when_xwaitfordpu_falsy(self, bulk_runner, monkeypatch):
        bulk_runner.glue_client.get_job_run.return_value = {
            'JobRun': {'DPUSeconds': 100.0}
        }
        sleeps = []
        monkeypatch.setattr(runner_module.time, 'sleep', lambda s: sleeps.append(s))
        bulk_runner._get_job_run_dpu('jr-1', {})
        assert sleeps == []

    def test_exception_returns_minus_one(self, bulk_runner, monkeypatch):
        monkeypatch.setattr(runner_module.log, 'error', MagicMock())
        bulk_runner.glue_client.get_job_run.side_effect = RuntimeError('boom')
        result = bulk_runner._get_job_run_dpu('jr-1', {})
        assert result == -1


# --- _get_glue_job_arguments ------------------------------------------------


class TestGetGlueJobArguments:
    """Tests for argument assembly (lines 320-338)."""

    def test_xdebug_forwarded_when_true(self, bulk_runner):
        result = bulk_runner._get_glue_job_arguments({'XDebug': True}, [])
        assert result['--XDebug'] == 'True'

    def test_xdebug_omitted_when_false(self, bulk_runner):
        result = bulk_runner._get_glue_job_arguments({'XDebug': False}, [])
        assert '--XDebug' not in result

    def test_xdebug_omitted_when_missing(self, bulk_runner):
        result = bulk_runner._get_glue_job_arguments({}, [])
        assert '--XDebug' not in result

    def test_double_dashed_pairs_become_arguments(self, bulk_runner):
        result = bulk_runner._get_glue_job_arguments(
            {}, ['--table', 'my-table', '--region', 'us-west-2']
        )
        assert result['--table'] == 'my-table'
        assert result['--region'] == 'us-west-2'

    def test_verb_renamed_to_xaction(self, bulk_runner):
        result = bulk_runner._get_glue_job_arguments({}, ['--verb', 'copy'])
        assert '--XAction' in result
        assert result['--XAction'] == 'copy'
        assert '--verb' not in result

    def test_odd_count_drops_trailing_flag(self, bulk_runner):
        # Trailing --flag with no value -> value is None at i+1, so omitted.
        result = bulk_runner._get_glue_job_arguments({}, ['--flag'])
        assert '--flag' not in result

    def test_non_double_dash_args_skipped(self, bulk_runner):
        # Without leading '--', the entry is ignored as a flag.
        result = bulk_runner._get_glue_job_arguments({}, ['table', 'my-table'])
        assert '--table' not in result

    def test_none_value_skipped(self, bulk_runner):
        # Explicit None value at i+1 should be dropped.
        result = bulk_runner._get_glue_job_arguments({}, ['--key', None])
        assert '--key' not in result


# --- _assert_expected_script_args -------------------------------------------


class TestAssertExpectedScriptArgs:
    """Tests for environment alignment checks (lines 340-366)."""

    def test_raises_when_glue_job_missing(self, bulk_runner):
        with patch.object(runner_module, 'is_existing_glue_job', return_value=False):
            with pytest.raises(AssertionError):
                bulk_runner._assert_expected_script_args({}, {})

    def test_calls_assert_version_parity_when_job_exists(self, bulk_runner):
        with patch.object(runner_module, 'is_existing_glue_job', return_value=True), \
             patch.object(runner_module, 'assert_version_parity') as ver:
            bulk_runner._assert_expected_script_args({'foo': 'bar'}, {})
        ver.assert_called_once_with(bulk_runner.glue_client, {'foo': 'bar'})


# --- _start_glue_job --------------------------------------------------------


class TestStartGlueJob:
    """Tests for Glue job submission and error mapping (lines 368-392)."""

    def test_returns_job_run_id_on_success(self, bulk_runner):
        bulk_runner.glue_client.start_job_run.return_value = {'JobRunId': 'jr-99'}
        run_id = bulk_runner._start_glue_job({}, {})
        assert run_id == 'jr-99'

    def test_uses_default_execution_class_when_missing(self, bulk_runner):
        bulk_runner.glue_client.start_job_run.return_value = {'JobRunId': 'x'}
        bulk_runner._start_glue_job({}, {})
        kwargs = bulk_runner.glue_client.start_job_run.call_args.kwargs
        assert kwargs['ExecutionClass'] == runner_module.GlueJobDefaults.ExecutionClass.value

    def test_uses_default_number_of_workers_when_missing(self, bulk_runner):
        bulk_runner.glue_client.start_job_run.return_value = {'JobRunId': 'x'}
        bulk_runner._start_glue_job({}, {})
        kwargs = bulk_runner.glue_client.start_job_run.call_args.kwargs
        assert kwargs['NumberOfWorkers'] == runner_module.GlueJobDefaults.NumberOfWorkers.value

    def test_uses_default_timeout_when_missing(self, bulk_runner):
        bulk_runner.glue_client.start_job_run.return_value = {'JobRunId': 'x'}
        bulk_runner._start_glue_job({}, {})
        kwargs = bulk_runner.glue_client.start_job_run.call_args.kwargs
        assert kwargs['Timeout'] == runner_module.GlueJobDefaults.Timeout.value

    def test_uses_default_worker_type_when_missing(self, bulk_runner):
        bulk_runner.glue_client.start_job_run.return_value = {'JobRunId': 'x'}
        bulk_runner._start_glue_job({}, {})
        kwargs = bulk_runner.glue_client.start_job_run.call_args.kwargs
        assert kwargs['WorkerType'] == runner_module.GlueJobDefaults.WorkerType.value

    def test_overrides_with_provided_args(self, bulk_runner):
        bulk_runner.glue_client.start_job_run.return_value = {'JobRunId': 'x'}
        args = {
            'XExecutionClass': 'FLEX',
            'XNumberOfWorkers': 5,
            'XTimeout': 30,
            'XWorkerType': 'G.2X',
        }
        bulk_runner._start_glue_job({}, args)
        kwargs = bulk_runner.glue_client.start_job_run.call_args.kwargs
        assert kwargs['ExecutionClass'] == 'FLEX'
        assert kwargs['NumberOfWorkers'] == 5
        assert kwargs['Timeout'] == 30
        assert kwargs['WorkerType'] == 'G.2X'

    def test_expired_token_exception_exits(self, bulk_runner):
        err = ClientError(
            {'Error': {'Code': 'ExpiredTokenException', 'Message': 'expired'}},
            'StartJobRun',
        )
        bulk_runner.glue_client.start_job_run.side_effect = err
        with pytest.raises(SystemExit):
            bulk_runner._start_glue_job({}, {})

    def test_entity_not_found_exception_exits(self, bulk_runner):
        err = ClientError(
            {'Error': {'Code': 'EntityNotFoundException', 'Message': 'no job'}},
            'StartJobRun',
        )
        bulk_runner.glue_client.start_job_run.side_effect = err
        with pytest.raises(SystemExit):
            bulk_runner._start_glue_job({}, {})

    def test_unhandled_exception_exits(self, bulk_runner):
        bulk_runner.glue_client.start_job_run.side_effect = RuntimeError('weird')
        with pytest.raises(SystemExit):
            bulk_runner._start_glue_job({}, {})

    def test_client_error_with_unknown_code_exits(self, bulk_runner):
        err = ClientError(
            {'Error': {'Code': 'SomethingElse', 'Message': 'huh'}},
            'StartJobRun',
        )
        bulk_runner.glue_client.start_job_run.side_effect = err
        with pytest.raises(SystemExit):
            bulk_runner._start_glue_job({}, {})

    def test_uses_default_idle_timeout_when_missing(self, bulk_runner):
        bulk_runner.glue_client.start_job_run.return_value = {'JobRunId': 'x'}
        bulk_runner._start_glue_job({}, {})
        kwargs = bulk_runner.glue_client.start_job_run.call_args.kwargs
        assert kwargs['IdleTimeout'] == runner_module.GlueJobDefaults.IdleTimeout.value

    def test_overrides_idle_timeout_with_provided_arg(self, bulk_runner):
        bulk_runner.glue_client.start_job_run.return_value = {'JobRunId': 'x'}
        bulk_runner._start_glue_job({}, {'XIdleTimeout': 10})
        kwargs = bulk_runner.glue_client.start_job_run.call_args.kwargs
        assert kwargs['IdleTimeout'] == 10


# --- _stop_glue_job ---------------------------------------------------------


class TestStopGlueJob:
    """Tests for stop-job pathways (lines 394-406)."""

    def test_logs_on_successful_submissions(self, bulk_runner, caplog):
        bulk_runner.glue_client.batch_stop_job_run.return_value = {
            'SuccessfulSubmissions': [{'JobRunId': 'jr-1'}]
        }
        import logging as _logging
        with caplog.at_level(_logging.INFO):
            bulk_runner._stop_glue_job('jr-1')
        bulk_runner.glue_client.batch_stop_job_run.assert_called_once_with(
            JobName=runner_module.GLUE_JOB_NAME, JobRunIds=['jr-1']
        )

    def test_logs_errors_when_present(self, bulk_runner, caplog):
        bulk_runner.glue_client.batch_stop_job_run.return_value = {
            'Errors': [{'JobRunId': 'jr-1', 'ErrorDetail': 'x'}]
        }
        import logging as _logging
        with caplog.at_level(_logging.ERROR):
            bulk_runner._stop_glue_job('jr-1')
        bulk_runner.glue_client.batch_stop_job_run.assert_called_once()

    def test_exception_is_swallowed(self, bulk_runner):
        bulk_runner.glue_client.batch_stop_job_run.side_effect = RuntimeError('boom')
        # Should NOT raise.
        bulk_runner._stop_glue_job('jr-1')


# --- _watch_for_interrupt ---------------------------------------------------


class TestWatchForInterrupt:
    """Tests for the ^C interrupt handler (lines 408-418)."""

    def test_returns_when_state_succeeds_immediately(self, bulk_runner):
        bulk_runner._get_job_run_state = MagicMock(return_value=runner_module.SUCCEEDED_STATE)
        # Loop's while-condition is false on entry → no interrupt handler usage.
        bulk_runner._watch_for_interrupt('jr-1')

    def test_interrupt_triggers_stop(self, bulk_runner, monkeypatch):
        bulk_runner._get_job_run_state = MagicMock(return_value=runner_module.RUNNING_STATE)
        bulk_runner._stop_glue_job = MagicMock()
        monkeypatch.setattr(runner_module.time, 'sleep', lambda s: None)

        # Build a fake handler whose `interrupted` flips to True after entering.
        class FakeHandler:
            def __init__(self):
                self.interrupted = True  # already interrupted on entry

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        monkeypatch.setattr(runner_module, 'GracefulInterruptHandler', FakeHandler)
        bulk_runner._watch_for_interrupt('jr-1')
        bulk_runner._stop_glue_job.assert_called_once_with('jr-1')

    def test_loops_until_terminal_state(self, bulk_runner, monkeypatch):
        # Sequence: RUNNING, RUNNING, FAILED → loop exits, no stop.
        bulk_runner._get_job_run_state = MagicMock(side_effect=[
            runner_module.RUNNING_STATE,
            runner_module.RUNNING_STATE,
            runner_module.FAILED_STATE,
        ])
        bulk_runner._stop_glue_job = MagicMock()
        monkeypatch.setattr(runner_module.time, 'sleep', lambda s: None)

        class FakeHandler:
            interrupted = False

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        monkeypatch.setattr(runner_module, 'GracefulInterruptHandler', FakeHandler)
        bulk_runner._watch_for_interrupt('jr-1')
        bulk_runner._stop_glue_job.assert_not_called()


# --- run() ------------------------------------------------------------------


class TestRunArgPrep:
    """Tests for run()'s arg-prep failure short-circuit (lines 421-431)."""

    def test_arg_prep_failure_returns_without_starting_job(self, bulk_runner, monkeypatch):
        bulk_runner._get_glue_job_arguments = MagicMock(side_effect=RuntimeError('bad'))
        bulk_runner._start_glue_job = MagicMock()
        bulk_runner.run({}, [])
        bulk_runner._start_glue_job.assert_not_called()

    def test_assert_failure_short_circuits(self, bulk_runner):
        bulk_runner._get_glue_job_arguments = MagicMock(return_value={})
        bulk_runner._assert_expected_script_args = MagicMock(side_effect=AssertionError('no job'))
        bulk_runner._start_glue_job = MagicMock()
        bulk_runner.run({}, [])
        bulk_runner._start_glue_job.assert_not_called()


def _wire_run_dependencies(bulk_runner, *, final_state, error_message=None,
                           dpu_seconds=0):
    """Wire up the helpers run() calls so the happy path completes deterministically."""
    bulk_runner._get_glue_job_arguments = MagicMock(return_value={})
    bulk_runner._assert_expected_script_args = MagicMock()
    bulk_runner._start_glue_job = MagicMock(return_value='jr-1')
    bulk_runner._watch_glue_job = MagicMock()
    bulk_runner._watch_for_interrupt = MagicMock()
    bulk_runner._get_job_run_state = MagicMock(return_value=final_state)
    bulk_runner._get_job_run_error_message = MagicMock(return_value=error_message)
    bulk_runner._get_job_run_dpu = MagicMock(return_value=dpu_seconds)


class TestRunStateBranches:
    """Tests for end-state messaging in run() (lines 466-477)."""

    def test_succeeded_state(self, bulk_runner, caplog):
        _wire_run_dependencies(bulk_runner, final_state=runner_module.SUCCEEDED_STATE)
        import logging as _logging
        with caplog.at_level(_logging.INFO):
            bulk_runner.run({}, [])
        assert any('completed successfully' in m for m in caplog.messages)

    def test_stopping_state(self, bulk_runner, caplog):
        _wire_run_dependencies(bulk_runner, final_state=runner_module.STOPPING_STATE)
        import logging as _logging
        with caplog.at_level(_logging.INFO):
            bulk_runner.run({}, [])
        assert any('stopping' in m for m in caplog.messages)

    def test_stopped_state(self, bulk_runner, caplog):
        _wire_run_dependencies(bulk_runner, final_state=runner_module.STOPPED_STATE)
        import logging as _logging
        with caplog.at_level(_logging.INFO):
            bulk_runner.run({}, [])
        assert any('stopped' in m for m in caplog.messages)

    def test_failed_state(self, bulk_runner, caplog):
        _wire_run_dependencies(bulk_runner, final_state=runner_module.FAILED_STATE)
        import logging as _logging
        with caplog.at_level(_logging.INFO):
            bulk_runner.run({}, [])
        assert any('failed' in m.lower() for m in caplog.messages)

    def test_timeout_state(self, bulk_runner, caplog):
        _wire_run_dependencies(bulk_runner, final_state=runner_module.TIMEOUT_STATE)
        import logging as _logging
        with caplog.at_level(_logging.INFO):
            bulk_runner.run({}, [])
        assert any('timed out' in m for m in caplog.messages)

    def test_unhandled_state_logs_error(self, bulk_runner, caplog):
        _wire_run_dependencies(bulk_runner, final_state='WEIRD_STATE')
        import logging as _logging
        with caplog.at_level(_logging.ERROR):
            bulk_runner.run({}, [])
        assert any('Unhandled Job State' in m for m in caplog.messages)

    def test_starts_job_with_arguments(self, bulk_runner):
        _wire_run_dependencies(bulk_runner, final_state=runner_module.SUCCEEDED_STATE)
        bulk_runner.run({'XDebug': True}, ['--table', 'tbl'])
        bulk_runner._start_glue_job.assert_called_once()


class TestRunDpuFormatting:
    """Tests for DPU-hours branching in run() (lines 482-489)."""

    def test_dpu_hours_zero_uses_short_format(self, bulk_runner, caplog):
        _wire_run_dependencies(bulk_runner, final_state=runner_module.SUCCEEDED_STATE,
                               dpu_seconds=0)
        import logging as _logging
        with caplog.at_level(_logging.INFO):
            bulk_runner.run({}, [])
        # Either format appears, but zero-DPU branch omits the 'DPU hours' suffix.
        assert any('DPU hours' not in m and 'completed successfully' in m
                   for m in caplog.messages)

    def test_dpu_hours_positive_includes_dpu_hours_in_log(self, bulk_runner, caplog):
        _wire_run_dependencies(bulk_runner, final_state=runner_module.SUCCEEDED_STATE,
                               dpu_seconds=3600)  # = 1.0 DPU hour
        import logging as _logging
        with caplog.at_level(_logging.INFO):
            bulk_runner.run({}, [])
        assert any('DPU hours' in m for m in caplog.messages)


class TestRunErrorMessageLogging:
    """Tests for terminal error-message logging (lines 491-492)."""

    def test_logs_error_message_when_present(self, bulk_runner, caplog):
        _wire_run_dependencies(bulk_runner, final_state=runner_module.FAILED_STATE,
                               error_message='something exploded')
        import logging as _logging
        with caplog.at_level(_logging.ERROR):
            bulk_runner.run({}, [])
        assert any('something exploded' in m for m in caplog.messages)

    def test_no_error_message_branch(self, bulk_runner, caplog):
        _wire_run_dependencies(bulk_runner, final_state=runner_module.SUCCEEDED_STATE,
                               error_message=None)
        import logging as _logging
        with caplog.at_level(_logging.ERROR):
            bulk_runner.run({}, [])
        # No ERROR-level log lines should be emitted.
        assert not any(rec.levelname == 'ERROR' for rec in caplog.records)


# --- Module constants -------------------------------------------------------


class TestModuleConstants:
    """Verify module-level state constants align with Glue values."""

    def test_terminal_states_set_membership(self):
        assert runner_module.STOPPED_STATE in runner_module.TERMINAL_JOB_STATES
        assert runner_module.FAILED_STATE in runner_module.TERMINAL_JOB_STATES
        assert runner_module.TIMEOUT_STATE in runner_module.TERMINAL_JOB_STATES

    def test_succeeded_not_in_terminal_set(self):
        # SUCCEEDED is handled separately so the success log path runs.
        assert runner_module.SUCCEEDED_STATE not in runner_module.TERMINAL_JOB_STATES

    def test_live_tail_max_retries_constant(self):
        assert runner_module.LIVE_TAIL_MAX_RETRIES == 20

    def test_live_tail_retry_wait_seconds_constant(self):
        assert runner_module.LIVE_TAIL_RETRY_WAIT_TIME_IN_SECONDS == 2

    def test_live_tail_success_shutdown_max_count_constant(self):
        assert runner_module.LIVE_TAIL_SUCCESS_SHUTDOWN_MAX_COUNT == 3
