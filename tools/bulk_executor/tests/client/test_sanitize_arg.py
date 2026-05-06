"""Unit tests for sanitize_arg."""

from utils import sanitize_arg


class TestSanitizeArg:

    def test_strip_py_extension(self):
        assert sanitize_arg("load_only_active.py", r'\.py$') == "load_only_active"

    def test_no_py_extension(self):
        assert sanitize_arg("example", r'\.py$') == "example"

    def test_py_in_middle_not_stripped(self):
        assert sanitize_arg("my.py.module", r'\.py$') == "my.py.module"

    def test_strip_trailing_slashes(self):
        assert sanitize_arg("s3://bucket/path/", r'/+$') == "s3://bucket/path"

    def test_no_trailing_slash(self):
        assert sanitize_arg("s3://bucket/path", r'/+$') == "s3://bucket/path"

    def test_custom_replacement(self):
        assert sanitize_arg("hello world", r'\s+', '_') == "hello_world"

    def test_empty_string(self):
        assert sanitize_arg("", r'\.py$') == ""
