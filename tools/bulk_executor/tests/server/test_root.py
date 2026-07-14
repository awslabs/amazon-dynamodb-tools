"""Unit tests for `server/src/root.py` — the Glue job dispatcher entrypoint.

Covers `server/src/root.py`:
- `_get_first_system_exit_line`: marker scanning across formatted tracebacks
  (no exit, single-line exit, message after marker, multi-line traceback).
- `_get_parsed_glue_job_args`: argv parsing for `--key value` pairs, optional
  flag-without-value handling, and the XDebug print-on-true branch.
- Module-level dispatcher logic: SparkContext/GlueContext/Job initialization,
  sys.path append, XAction → module name mapping (default + dash-to-underscore
  rewrite), logger init wiring, importlib import_module dispatch, the success
  path that calls `module.run(job, sc, gc, parsed_args)`, the missing-module
  ImportError → "Could not find action" path, the missing-`run`-function path,
  the BulkExecutorError re-raise branch, the generic-Exception re-raise branch,
  and the final `job.commit()` + `spark_context.stop()` cleanup.

root.py executes everything at import time, so each test reloads a fresh
module via `importlib.util.spec_from_file_location` after pre-populating
`sys.modules` with stand-ins for awsglue / pyspark / verb modules and
configuring `sys.argv` to the scenario under test.

The existing tests/server/conftest.py mocks awsglue, pyspark, and shared
modules globally; these tests further customize the awsglue / verb-module
entries per-test so the side effects are observable.
"""

import importlib.util
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# --- Constants & helpers ----------------------------------------------------

ROOT_PY_PATH = (
    Path(__file__).resolve().parents[2] / "server" / "src" / "root.py"
)


def _build_awsglue_stubs():
    """Build fresh awsglue/pyspark module stand-ins with observable contexts."""
    spark_ctx = MagicMock(name="SparkContext_instance")
    glue_ctx = MagicMock(name="GlueContext_instance")
    job_instance = MagicMock(name="Job_instance")

    spark_module = types.ModuleType("pyspark")
    spark_context_module = types.ModuleType("pyspark.context")
    sc_class = MagicMock(name="SparkContext_class")
    sc_class.getOrCreate = MagicMock(return_value=spark_ctx)
    spark_context_module.SparkContext = sc_class

    awsglue_module = types.ModuleType("awsglue")
    awsglue_context_module = types.ModuleType("awsglue.context")
    glue_context_class = MagicMock(name="GlueContext_class", return_value=glue_ctx)
    awsglue_context_module.GlueContext = glue_context_class

    awsglue_job_module = types.ModuleType("awsglue.job")
    job_class = MagicMock(name="Job_class", return_value=job_instance)
    awsglue_job_module.Job = job_class

    awsglue_transforms_module = types.ModuleType("awsglue.transforms")
    # `from awsglue.transforms import *` requires only a module — empty is fine.

    return {
        "modules": {
            "pyspark": spark_module,
            "pyspark.context": spark_context_module,
            "awsglue": awsglue_module,
            "awsglue.context": awsglue_context_module,
            "awsglue.job": awsglue_job_module,
            "awsglue.transforms": awsglue_transforms_module,
        },
        "spark_ctx": spark_ctx,
        "glue_ctx": glue_ctx,
        "job_instance": job_instance,
        "sc_class": sc_class,
        "glue_context_class": glue_context_class,
        "job_class": job_class,
    }


def _install_logger_stub(modules_to_install):
    """Logger needs `init` and `log` attrs; both must survive star imports."""
    logger_module = types.ModuleType("python_modules.shared.logger")
    logger_module.init = MagicMock(name="logger_init")
    logger_module.log = MagicMock(name="logger_log")
    modules_to_install["python_modules.shared.logger"] = logger_module
    return logger_module


def _install_bulk_executor_error_stub(modules_to_install):
    """Provide a real exception class so `except BulkExecutorError` actually matches."""
    be_module = types.ModuleType("python_modules.shared.bulk_executor_error")

    class BulkExecutorError(Exception):
        pass

    be_module.BulkExecutorError = BulkExecutorError
    modules_to_install["python_modules.shared.bulk_executor_error"] = be_module
    return BulkExecutorError


def _load_root(monkeypatch, argv, verb_module=None, verb_name=None,
               import_should_fail=False, awsglue_stubs=None,
               logger_module=None):
    """Execute root.py freshly with the supplied argv and verb wiring.

    Returns a dict with the executed module plus the stubs that were
    installed so tests can assert on init/dispatch/cleanup calls.
    """
    monkeypatch.setattr(sys, "argv", list(argv))

    if awsglue_stubs is None:
        awsglue_stubs = _build_awsglue_stubs()

    install = dict(awsglue_stubs["modules"])

    if logger_module is None:
        logger_module = _install_logger_stub(install)

    BulkExecutorError = _install_bulk_executor_error_stub(install)

    # The verb module is what root.py imports via importlib.import_module.
    # If `import_should_fail` is set, we leave python_modules.<verb_name>
    # uninstalled and patch importlib.import_module to raise ImportError so
    # the dispatcher path runs deterministically (otherwise importlib would
    # try to find a real package on disk).
    for name, module in install.items():
        monkeypatch.setitem(sys.modules, name, module)

    # Make sure the target verb module isn't lingering from a previous test.
    candidate_names = ["python_modules.copy", "python_modules.count",
                       "python_modules.delete", "python_modules.fill",
                       "python_modules.default", "python_modules.fake_action",
                       "python_modules.dash_action", "python_modules.no_run"]
    for cand in candidate_names:
        monkeypatch.delitem(sys.modules, cand, raising=False)

    if verb_module is not None and verb_name is not None and not import_should_fail:
        monkeypatch.setitem(sys.modules,
                            f"python_modules.{verb_name}",
                            verb_module)

    # Always wipe a previously-loaded root module so spec exec runs cleanly.
    monkeypatch.delitem(sys.modules, "_root_under_test", raising=False)

    spec = importlib.util.spec_from_file_location(
        "_root_under_test", str(ROOT_PY_PATH))
    module = importlib.util.module_from_spec(spec)

    if import_should_fail:
        original = importlib.import_module

        def fake_import(name, package=None):
            if name.startswith("python_modules.") and name != "python_modules.shared.logger":
                raise ImportError(f"forced failure for {name}")
            return original(name, package)

        with patch("importlib.import_module", side_effect=fake_import):
            spec.loader.exec_module(module)
    else:
        spec.loader.exec_module(module)

    return {
        "module": module,
        "awsglue": awsglue_stubs,
        "logger": logger_module,
        "BulkExecutorError": BulkExecutorError,
    }


def _make_verb_module(name, run_callable=None, has_run=True):
    """Build a fake verb module with a controllable `run` callable."""
    mod = types.ModuleType(f"python_modules.{name}")
    if has_run:
        mod.run = run_callable if run_callable is not None else MagicMock(name=f"{name}.run")
    return mod


# --- _get_first_system_exit_line --------------------------------------------

class TestGetFirstSystemExitLine:
    """Tests for `_get_first_system_exit_line` (lines 12-22)."""

    def test_returns_none_when_no_exception_in_progress(self, monkeypatch):
        """Line 17, 22: no SystemExit in current trace → returns None."""
        # Need root loaded so the helper exists. Use a passing dispatch.
        verb = _make_verb_module("copy")
        ctx = _load_root(monkeypatch,
                         ["root.py", "--XAction", "copy"],
                         verb_module=verb, verb_name="copy")
        # Outside any except: format_exc() → "NoneType: None" → no marker.
        assert ctx["module"]._get_first_system_exit_line() is None

    def test_returns_message_after_marker(self, monkeypatch):
        """Lines 18-21: when SystemExit is in flight, returns text after 'SystemExit: '."""
        verb = _make_verb_module("copy")
        ctx = _load_root(monkeypatch,
                         ["root.py", "--XAction", "copy"],
                         verb_module=verb, verb_name="copy")
        try:
            raise SystemExit("table not found")
        except SystemExit:
            assert ctx["module"]._get_first_system_exit_line() == "table not found"

    def test_returns_none_when_traceback_has_no_systemexit(self, monkeypatch):
        """Line 22: a non-SystemExit exception still produces a traceback but no marker."""
        verb = _make_verb_module("copy")
        ctx = _load_root(monkeypatch,
                         ["root.py", "--XAction", "copy"],
                         verb_module=verb, verb_name="copy")
        try:
            raise ValueError("nope")
        except ValueError:
            assert ctx["module"]._get_first_system_exit_line() is None


# --- _get_parsed_glue_job_args ----------------------------------------------

class TestGetParsedGlueJobArgs:
    """Tests for `_get_parsed_glue_job_args` (lines 24-51)."""

    @pytest.fixture
    def loaded(self, monkeypatch):
        """Load root.py once for all parser tests."""
        verb = _make_verb_module("copy")
        return _load_root(monkeypatch,
                          ["root.py", "--XAction", "copy"],
                          verb_module=verb, verb_name="copy")

    def test_parses_simple_key_value(self, loaded):
        """Lines 39-43: `--key value` pair becomes parsed_args[key] = value."""
        parsed = loaded["module"]._get_parsed_glue_job_args(
            ["root.py", "--table", "my-table"])
        assert parsed == {"table": "my-table"}

    def test_parses_multiple_key_values(self, loaded):
        """Iterates argv across many pairs."""
        parsed = loaded["module"]._get_parsed_glue_job_args(
            ["root.py", "--XAction", "copy", "--table", "tbl", "--region", "us-east-1"])
        assert parsed == {"XAction": "copy", "table": "tbl", "region": "us-east-1"}

    def test_strips_leading_double_dashes_from_key(self, loaded):
        """Line 41: lstrip('--') drops leading dashes from the key."""
        parsed = loaded["module"]._get_parsed_glue_job_args(
            ["script", "--XAction", "copy"])
        assert "XAction" in parsed and "--XAction" not in parsed

    def test_flag_without_value_followed_by_another_flag_gets_none(self, loaded):
        """Lines 45-47: a flag whose next argv entry is also a flag → value = None."""
        parsed = loaded["module"]._get_parsed_glue_job_args(
            ["script", "--debug", "--table", "tbl"])
        assert parsed["debug"] is None
        assert parsed["table"] == "tbl"

    def test_trailing_flag_without_value_gets_none(self, loaded):
        """Lines 42, 46-47: final argv element with no value → None."""
        parsed = loaded["module"]._get_parsed_glue_job_args(
            ["script", "--solo"])
        assert parsed == {"solo": None}

    def test_skips_script_name_at_index_zero(self, loaded):
        """Line 38: parser starts at i=1, ignoring argv[0]."""
        parsed = loaded["module"]._get_parsed_glue_job_args(
            ["my-script.py", "--key", "val"])
        assert parsed == {"key": "val"}
        assert "my-script.py" not in parsed

    def test_empty_argv_returns_empty_dict(self, loaded):
        """Line 39: while loop never enters when argv has only the script name (or less)."""
        assert loaded["module"]._get_parsed_glue_job_args([]) == {}
        assert loaded["module"]._get_parsed_glue_job_args(["script"]) == {}

    def test_xdebug_truthy_prints_parsed_args(self, loaded, capsys):
        """Lines 49-50: XDebug=truthy triggers the print() side-effect."""
        loaded["module"]._get_parsed_glue_job_args(
            ["script", "--XDebug", "1", "--table", "tbl"])
        captured = capsys.readouterr().out
        assert "Parsed arguments:" in captured
        assert "table" in captured

    def test_xdebug_falsy_does_not_print(self, loaded, capsys):
        """Line 49: XDebug missing or falsy skips the debug print."""
        loaded["module"]._get_parsed_glue_job_args(
            ["script", "--table", "tbl"])
        captured = capsys.readouterr().out
        assert "Parsed arguments:" not in captured

    def test_argument_after_dashed_value_treated_as_orphan_flag(self, loaded):
        """Line 42: a value that itself starts with -- is NOT consumed as the value."""
        parsed = loaded["module"]._get_parsed_glue_job_args(
            ["script", "--first", "--second", "value"])
        # `--first` has no value (next is `--second`), `--second` gets `value`
        assert parsed["first"] is None
        assert parsed["second"] == "value"


# --- Module-level dispatcher ------------------------------------------------

class TestRootInitialization:
    """Spark/Glue/Job init and sys.path setup (lines 55-62)."""

    def test_spark_context_get_or_create_called(self, monkeypatch):
        """Line 55: SparkContext.getOrCreate() is invoked at import."""
        verb = _make_verb_module("copy")
        ctx = _load_root(monkeypatch,
                         ["root.py", "--XAction", "copy"],
                         verb_module=verb, verb_name="copy")
        ctx["awsglue"]["sc_class"].getOrCreate.assert_called_once_with()

    def test_glue_context_constructed_with_spark_context(self, monkeypatch):
        """Line 56: GlueContext(spark_context)."""
        verb = _make_verb_module("copy")
        ctx = _load_root(monkeypatch,
                         ["root.py", "--XAction", "copy"],
                         verb_module=verb, verb_name="copy")
        ctx["awsglue"]["glue_context_class"].assert_called_once_with(
            ctx["awsglue"]["spark_ctx"])

    def test_job_constructed_with_glue_context(self, monkeypatch):
        """Line 57: Job(glue_context)."""
        verb = _make_verb_module("copy")
        ctx = _load_root(monkeypatch,
                         ["root.py", "--XAction", "copy"],
                         verb_module=verb, verb_name="copy")
        ctx["awsglue"]["job_class"].assert_called_once_with(
            ctx["awsglue"]["glue_ctx"])

    def test_python_modules_appended_to_sys_path(self, monkeypatch):
        """Line 62: sys.path.append('python_modules')."""
        verb = _make_verb_module("copy")
        _load_root(monkeypatch,
                   ["root.py", "--XAction", "copy"],
                   verb_module=verb, verb_name="copy")
        assert "python_modules" in sys.path


class TestRootLoggerInit:
    """Logger init wiring (lines 71-72)."""

    def test_logger_init_called_with_parsed_args(self, monkeypatch):
        """Line 72: init(parsed_args) is invoked with the dict from argv parsing."""
        verb = _make_verb_module("copy")
        ctx = _load_root(monkeypatch,
                         ["root.py", "--XAction", "copy", "--table", "tbl"],
                         verb_module=verb, verb_name="copy")
        ctx["logger"].init.assert_called_once()
        passed = ctx["logger"].init.call_args.args[0]
        assert passed["XAction"] == "copy"
        assert passed["table"] == "tbl"


class TestRootActionResolution:
    """XAction → module-name mapping (lines 65-67)."""

    def test_default_action_when_xaction_missing(self, monkeypatch):
        """Line 65: missing XAction defaults to 'default'."""
        verb = _make_verb_module("default")
        ctx = _load_root(monkeypatch,
                         ["root.py", "--table", "tbl"],
                         verb_module=verb, verb_name="default")
        verb.run.assert_called_once()

    def test_dashes_in_xaction_replaced_with_underscores(self, monkeypatch):
        """Line 65: action_module = XAction.replace('-', '_')."""
        verb = _make_verb_module("dash_action")
        ctx = _load_root(monkeypatch,
                         ["root.py", "--XAction", "dash-action"],
                         verb_module=verb, verb_name="dash_action")
        verb.run.assert_called_once()

    def test_module_name_prefixed_with_python_modules(self, monkeypatch):
        """Line 67: module_name is f'python_modules.{action_module}'."""
        verb = _make_verb_module("count")
        ctx = _load_root(monkeypatch,
                         ["root.py", "--XAction", "count"],
                         verb_module=verb, verb_name="count")
        # If the prefixing wasn't right, importlib would raise ImportError
        # (we registered the module under python_modules.count).
        verb.run.assert_called_once()


class TestRootSuccessfulDispatch:
    """The happy-path dispatch into a verb module (lines 74-89, 93-94)."""

    def test_run_called_with_job_sc_gc_parsed_args(self, monkeypatch):
        """Line 85: action_script_function(job, spark_context, glue_context, parsed_args)."""
        run_mock = MagicMock(name="copy.run")
        verb = _make_verb_module("copy", run_callable=run_mock)
        ctx = _load_root(monkeypatch,
                         ["root.py", "--XAction", "copy", "--table", "tbl"],
                         verb_module=verb, verb_name="copy")
        run_mock.assert_called_once()
        args = run_mock.call_args.args
        assert args[0] is ctx["awsglue"]["job_instance"]
        assert args[1] is ctx["awsglue"]["spark_ctx"]
        assert args[2] is ctx["awsglue"]["glue_ctx"]
        assert isinstance(args[3], dict)
        assert args[3]["XAction"] == "copy"
        assert args[3]["table"] == "tbl"

    def test_job_commit_called_after_successful_dispatch(self, monkeypatch):
        """Line 93: job.commit() runs only when dispatch succeeds."""
        verb = _make_verb_module("copy")
        ctx = _load_root(monkeypatch,
                         ["root.py", "--XAction", "copy"],
                         verb_module=verb, verb_name="copy")
        ctx["awsglue"]["job_instance"].commit.assert_called_once_with()

    def test_spark_context_stop_called_after_successful_dispatch(self, monkeypatch):
        """Line 94: spark_context.stop() runs at the end of the success path."""
        verb = _make_verb_module("copy")
        ctx = _load_root(monkeypatch,
                         ["root.py", "--XAction", "copy"],
                         verb_module=verb, verb_name="copy")
        ctx["awsglue"]["spark_ctx"].stop.assert_called_once_with()


class TestRootImportFailure:
    """ImportError → 'Could not find action' branch (lines 75-79)."""

    def test_missing_module_raises_could_not_find_action(self, monkeypatch):
        """Lines 76-79: ImportError on import_module → re-raises with action name."""
        with pytest.raises(Exception, match="Could not find action 'fake_action'"):
            _load_root(monkeypatch,
                       ["root.py", "--XAction", "fake_action"],
                       verb_module=None, verb_name="fake_action",
                       import_should_fail=True)


class TestRootMissingRunFunction:
    """Module imports OK but lacks `run` (lines 82, 90-91)."""

    def test_missing_run_function_raises(self, monkeypatch):
        """Line 91: hasattr check fails → raises Exception about missing 'run'."""
        verb = _make_verb_module("no_run", has_run=False)
        with pytest.raises(Exception, match="Could not find the function 'run'"):
            _load_root(monkeypatch,
                       ["root.py", "--XAction", "no_run"],
                       verb_module=verb, verb_name="no_run")

    def test_missing_run_function_skips_commit_and_stop(self, monkeypatch):
        """Lines 93-94: when run is missing, the raise prevents commit/stop."""
        verb = _make_verb_module("no_run", has_run=False)
        awsglue = _build_awsglue_stubs()
        with pytest.raises(Exception, match="Could not find the function"):
            _load_root(monkeypatch,
                       ["root.py", "--XAction", "no_run"],
                       verb_module=verb, verb_name="no_run",
                       awsglue_stubs=awsglue)
        awsglue["job_instance"].commit.assert_not_called()
        awsglue["spark_ctx"].stop.assert_not_called()


class TestRootBulkExecutorErrorPropagation:
    """BulkExecutorError from a verb is logged and exits cleanly (lines 86-88)."""

    def test_bulk_executor_error_exits_cleanly(self, monkeypatch):
        """Lines 86-88: BulkExecutorError from run() is logged and sys.exit'd."""
        awsglue = _build_awsglue_stubs()
        install = dict(awsglue["modules"])
        logger_module = _install_logger_stub(install)
        BulkExecutorError = _install_bulk_executor_error_stub(install)

        run_mock = MagicMock(side_effect=BulkExecutorError("user error"))
        verb = _make_verb_module("copy", run_callable=run_mock)

        for name, module in install.items():
            monkeypatch.setitem(sys.modules, name, module)
        monkeypatch.setitem(sys.modules, "python_modules.copy", verb)
        monkeypatch.setattr(sys, "argv", ["root.py", "--XAction", "copy"])

        spec = importlib.util.spec_from_file_location(
            "_root_under_test", str(ROOT_PY_PATH))
        module = importlib.util.module_from_spec(spec)
        with pytest.raises(SystemExit) as exc_info:
            spec.loader.exec_module(module)

        assert "user error" in str(exc_info.value)
        run_mock.assert_called_once()
        awsglue["job_instance"].commit.assert_not_called()
        awsglue["spark_ctx"].stop.assert_not_called()


class TestRootGenericExceptionPropagation:
    """Generic Exception from a verb is re-raised (lines 88-89)."""

    def test_generic_exception_propagates(self, monkeypatch):
        """Lines 88-89: a non-BulkExecutorError exception is re-raised verbatim."""
        run_mock = MagicMock(side_effect=RuntimeError("boom"))
        verb = _make_verb_module("copy", run_callable=run_mock)
        with pytest.raises(RuntimeError, match="boom"):
            _load_root(monkeypatch,
                       ["root.py", "--XAction", "copy"],
                       verb_module=verb, verb_name="copy")

    def test_generic_exception_skips_commit_and_stop(self, monkeypatch):
        """Lines 93-94: exception path bypasses job.commit() and spark.stop()."""
        run_mock = MagicMock(side_effect=ValueError("nope"))
        verb = _make_verb_module("copy", run_callable=run_mock)
        awsglue = _build_awsglue_stubs()
        with pytest.raises(ValueError):
            _load_root(monkeypatch,
                       ["root.py", "--XAction", "copy"],
                       verb_module=verb, verb_name="copy",
                       awsglue_stubs=awsglue)
        awsglue["job_instance"].commit.assert_not_called()
        awsglue["spark_ctx"].stop.assert_not_called()
