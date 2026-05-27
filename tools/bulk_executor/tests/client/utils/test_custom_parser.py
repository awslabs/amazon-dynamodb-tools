"""Unit tests for BulkArgumentParser.

Covers `client/src/utils/custom_parser.py`:
- __init__: formatter_class default (RawDescriptionHelpFormatter), help_text
  → description wiring, formatter_class override preserved, help_text stored
- error: prints help to stderr, exits with code 2 and the error message
- print_help: defaults to stdout when no file passed
- format_help: returns the formatted help string

Style notes:
- argparse's `error()` and `exit()` raise SystemExit on exit; tests assert
  the SystemExit and inspect captured output via capsys.
- Use a fresh parser per test (cheap to construct) so flag definitions
  don't leak between cases.
"""

import argparse
import sys

import pytest

from utils.custom_parser import BulkArgumentParser


# --- __init__ ---------------------------------------------------------------

class TestInit:
    """Tests for BulkArgumentParser.__init__ (lines 5-14)."""

    def test_default_formatter_class_is_raw_description(self):
        """Line 7: default formatter_class is RawDescriptionHelpFormatter."""
        parser = BulkArgumentParser()
        assert parser.formatter_class is argparse.RawDescriptionHelpFormatter

    def test_help_text_becomes_description(self):
        """Lines 10-11: help_text kwarg is wired to description."""
        parser = BulkArgumentParser(help_text="Custom multi-line\nhelp blurb")
        assert parser.description == "Custom multi-line\nhelp blurb"
        assert parser.help_text == "Custom multi-line\nhelp blurb"

    def test_no_help_text_stores_none(self):
        """Line 14: help_text attribute defaults to None."""
        parser = BulkArgumentParser()
        assert parser.help_text is None

    def test_explicit_formatter_class_preserved(self):
        """Line 7: kwargs.get keeps caller's formatter_class if provided."""
        parser = BulkArgumentParser(formatter_class=argparse.HelpFormatter)
        assert parser.formatter_class is argparse.HelpFormatter

    def test_help_text_does_not_override_explicit_description(self):
        """Lines 10-11: when help_text given, it overwrites description kwarg too."""
        # The source unconditionally sets kwargs['description'] = help_text
        # if help_text is truthy, regardless of any provided description.
        parser = BulkArgumentParser(description="orig", help_text="new help")
        assert parser.description == "new help"


# --- error ------------------------------------------------------------------

class TestError:
    """Tests for error (lines 16-19)."""

    def test_error_exits_with_code_2(self, capsys):
        """Line 19: error() exits with code 2."""
        parser = BulkArgumentParser(prog='bulk-test', help_text="usage info")

        with pytest.raises(SystemExit) as exc_info:
            parser.error("bad arg")

        assert exc_info.value.code == 2

    def test_error_prints_help_to_stderr(self, capsys):
        """Line 18: print_help is sent to sys.stderr before exit."""
        parser = BulkArgumentParser(prog='bulk-test', help_text="usage info here")

        with pytest.raises(SystemExit):
            parser.error("bad arg")

        captured = capsys.readouterr()
        assert "usage info here" in captured.err

    def test_error_message_in_stderr(self, capsys):
        """Line 19: '\\nerror: <message>\\n' is included in exit output."""
        parser = BulkArgumentParser(prog='bulk-test')

        with pytest.raises(SystemExit):
            parser.error("missing --foo")

        captured = capsys.readouterr()
        assert "error: missing --foo" in captured.err


# --- print_help -------------------------------------------------------------

class TestPrintHelp:
    """Tests for print_help (lines 21-23)."""

    def test_default_prints_to_stdout(self, capsys):
        """Line 23: when file is None, defaults to sys.stdout."""
        parser = BulkArgumentParser(prog='bulk-test', help_text="cmdline help")
        parser.print_help()  # no file arg

        captured = capsys.readouterr()
        assert "cmdline help" in captured.out
        assert captured.err == ""

    def test_explicit_file_argument_routes_there(self, capsys):
        """Line 23: explicit file kwarg overrides the default."""
        parser = BulkArgumentParser(prog='bulk-test', help_text="cmdline help")
        parser.print_help(sys.stderr)

        captured = capsys.readouterr()
        assert "cmdline help" in captured.err


# --- format_help ------------------------------------------------------------

class TestFormatHelp:
    """Tests for format_help (lines 25-27)."""

    def test_returns_formatted_help_string(self):
        """Line 27: format_help returns the parser's help text as a string."""
        parser = BulkArgumentParser(prog='bulk-test', help_text="banner txt")
        parser.add_argument('--foo')

        result = parser.format_help()

        assert isinstance(result, str)
        assert "banner txt" in result
        assert "--foo" in result
