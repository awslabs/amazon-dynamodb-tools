"""Unit tests for module_zipper.

Covers `client/src/utils/module_zipper.py`:
- zip_module(): delegates to _zip_module with the constants from
  infrastructure.constants
- _zip_module(): happy path (writes parent dir entry, recurses os.walk,
  writes files, writes subdir entries to preserve empty dirs, skips symlinks),
  exclusion of __pycache__, .DS_Store, .pyc/.pyo, and other junk,
  guard against zip_path inside source_path (raises ValueError → caught,
  returns False), exception during zipping (caught, log.error, returns False)

Style notes:
- Use the `tmp_path` fixture for filesystem tests so we exercise the real
  os.walk + zipfile.ZipFile machinery; this is the simplest way to verify
  the archive shape without mocking out the entire stdlib.
- For symlink-skip and the "zip inside tree" guard we control inputs
  directly; for the failure branch we patch zipfile.ZipFile to raise.
- log.info/log.error are patched at the module_zipper module namespace
  to avoid noisy stdout and to assert the success/failure log lines.
"""

import os
import zipfile
from unittest.mock import MagicMock, patch

import pytest

from utils import module_zipper


# --- zip_module thin wrapper ------------------------------------------------

class TestZipModule:
    """Tests for zip_module (line 12-13)."""

    def test_delegates_to_internal_with_constants(self, monkeypatch):
        """zip_module forwards the constants to _zip_module unchanged."""
        captured = {}

        def fake_inner(src, dst):
            captured['src'] = src
            captured['dst'] = dst
            return True

        monkeypatch.setattr(module_zipper, '_zip_module', fake_inner)
        monkeypatch.setattr(module_zipper, 'PYTHON_MODULE_CLIENT_DIR_PATH', 'src/dir')
        monkeypatch.setattr(module_zipper, 'PYTHON_MODULE_CLIENT_ZIP_PATH', 'out/zip.zip')

        assert module_zipper.zip_module() is True
        assert captured == {'src': 'src/dir', 'dst': 'out/zip.zip'}

    def test_returns_false_when_inner_fails(self, monkeypatch):
        """zip_module returns whatever _zip_module returns (False on failure)."""
        monkeypatch.setattr(module_zipper, '_zip_module', lambda *_args: False)
        assert module_zipper.zip_module() is False


# --- _zip_module ------------------------------------------------------------

class TestZipModuleInternalHappyPath:
    """Tests for _zip_module success path (lines 15-49)."""

    def test_basic_zip_creates_archive(self, tmp_path):
        """Zips a flat dir of two files; archive has parent-dir entry + both files."""
        source = tmp_path / "modules"
        source.mkdir()
        (source / "a.py").write_text("print('a')")
        (source / "b.py").write_text("print('b')")

        zip_path = tmp_path / "out.zip"

        with patch.object(module_zipper, 'log') as mock_log:
            result = module_zipper._zip_module(str(source), str(zip_path))

        assert result is True
        assert zip_path.exists()
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
        # Parent-dir entry
        assert 'modules/' in names
        # Both files under parent dir
        assert 'modules/a.py' in names
        assert 'modules/b.py' in names
        # Success message logged
        mock_log.info.assert_called_once()
        assert "Successfully zipped" in mock_log.info.call_args.args[0]

    def test_nested_dirs_preserved(self, tmp_path):
        """Nested directories and empty subdirs survive the round-trip."""
        source = tmp_path / "modules"
        source.mkdir()
        (source / "a.py").write_text("a")
        sub = source / "sub"
        sub.mkdir()
        (sub / "b.py").write_text("b")
        empty = source / "empty_dir"
        empty.mkdir()

        zip_path = tmp_path / "out.zip"

        with patch.object(module_zipper, 'log'):
            assert module_zipper._zip_module(str(source), str(zip_path)) is True

        with zipfile.ZipFile(zip_path) as zf:
            names = set(zf.namelist())
        assert 'modules/' in names
        assert 'modules/a.py' in names
        assert 'modules/sub/b.py' in names
        # Subdirectories get their own entry (line 43-46)
        assert any(n.startswith('modules/sub/') and n.endswith('/') for n in names)
        assert any('empty_dir' in n and n.endswith('/') for n in names)

    def test_symlink_files_are_skipped(self, tmp_path):
        """Symlinked files inside the source dir are skipped (line 35-36)."""
        source = tmp_path / "modules"
        source.mkdir()
        (source / "real.py").write_text("real")
        target = tmp_path / "outside.py"
        target.write_text("outside")
        link_path = source / "link.py"
        try:
            os.symlink(str(target), str(link_path))
        except (OSError, NotImplementedError):
            pytest.skip("symlinks not supported on this platform")

        zip_path = tmp_path / "out.zip"

        with patch.object(module_zipper, 'log'):
            assert module_zipper._zip_module(str(source), str(zip_path)) is True

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
        assert 'modules/real.py' in names
        # Symlink file not present
        assert 'modules/link.py' not in names

    def test_paths_normalized_to_absolute(self, tmp_path, monkeypatch):
        """Lines 18-19: relative input paths get abspath/normpath applied."""
        source = tmp_path / "modules"
        source.mkdir()
        (source / "a.py").write_text("a")

        # cd into tmp_path so we can pass relative paths
        monkeypatch.chdir(tmp_path)

        with patch.object(module_zipper, 'log'):
            assert module_zipper._zip_module('modules', 'out.zip') is True

        # Zip materialized at the absolute location
        assert (tmp_path / 'out.zip').exists()


class TestZipModuleExclusions:
    """Tests for file/directory exclusion logic."""

    def test_pycache_excluded(self, tmp_path):
        """__pycache__ directories and their contents are excluded."""
        source = tmp_path / "modules"
        source.mkdir()
        (source / "a.py").write_text("a")
        cache = source / "__pycache__"
        cache.mkdir()
        (cache / "a.cpython-311.pyc").write_bytes(b"\x00")

        zip_path = tmp_path / "out.zip"

        with patch.object(module_zipper, 'log'):
            assert module_zipper._zip_module(str(source), str(zip_path)) is True

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
        assert 'modules/a.py' in names
        assert not any('__pycache__' in n for n in names)

    def test_nested_pycache_excluded(self, tmp_path):
        """__pycache__ inside a subdirectory is also excluded."""
        source = tmp_path / "modules"
        source.mkdir()
        sub = source / "sub"
        sub.mkdir()
        (sub / "b.py").write_text("b")
        cache = sub / "__pycache__"
        cache.mkdir()
        (cache / "b.cpython-311.pyc").write_bytes(b"\x00")

        zip_path = tmp_path / "out.zip"

        with patch.object(module_zipper, 'log'):
            assert module_zipper._zip_module(str(source), str(zip_path)) is True

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
        assert 'modules/sub/b.py' in names
        assert not any('__pycache__' in n for n in names)

    def test_ds_store_excluded(self, tmp_path):
        """.DS_Store files are excluded from the archive."""
        source = tmp_path / "modules"
        source.mkdir()
        (source / "a.py").write_text("a")
        (source / ".DS_Store").write_bytes(b"\x00\x00\x00\x01")

        zip_path = tmp_path / "out.zip"

        with patch.object(module_zipper, 'log'):
            assert module_zipper._zip_module(str(source), str(zip_path)) is True

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
        assert 'modules/a.py' in names
        assert not any('.DS_Store' in n for n in names)

    def test_pyc_files_excluded(self, tmp_path):
        """.pyc files outside __pycache__ are also excluded."""
        source = tmp_path / "modules"
        source.mkdir()
        (source / "a.py").write_text("a")
        (source / "a.pyc").write_bytes(b"\x00")

        zip_path = tmp_path / "out.zip"

        with patch.object(module_zipper, 'log'):
            assert module_zipper._zip_module(str(source), str(zip_path)) is True

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
        assert 'modules/a.py' in names
        assert 'modules/a.pyc' not in names

    def test_pyo_files_excluded(self, tmp_path):
        """.pyo files are excluded."""
        source = tmp_path / "modules"
        source.mkdir()
        (source / "a.py").write_text("a")
        (source / "a.pyo").write_bytes(b"\x00")

        zip_path = tmp_path / "out.zip"

        with patch.object(module_zipper, 'log'):
            assert module_zipper._zip_module(str(source), str(zip_path)) is True

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
        assert 'modules/a.py' in names
        assert 'modules/a.pyo' not in names

    def test_egg_info_dir_excluded(self, tmp_path):
        """*.egg-info directories are excluded."""
        source = tmp_path / "modules"
        source.mkdir()
        (source / "a.py").write_text("a")
        egg = source / "pkg.egg-info"
        egg.mkdir()
        (egg / "PKG-INFO").write_text("info")

        zip_path = tmp_path / "out.zip"

        with patch.object(module_zipper, 'log'):
            assert module_zipper._zip_module(str(source), str(zip_path)) is True

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
        assert 'modules/a.py' in names
        assert not any('egg-info' in n for n in names)

    def test_thumbs_db_excluded(self, tmp_path):
        """Thumbs.db files are excluded."""
        source = tmp_path / "modules"
        source.mkdir()
        (source / "a.py").write_text("a")
        (source / "Thumbs.db").write_bytes(b"\x00")

        zip_path = tmp_path / "out.zip"

        with patch.object(module_zipper, 'log'):
            assert module_zipper._zip_module(str(source), str(zip_path)) is True

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
        assert 'modules/a.py' in names
        assert 'modules/Thumbs.db' not in names

    def test_pytest_cache_excluded(self, tmp_path):
        """.pytest_cache directories are excluded."""
        source = tmp_path / "modules"
        source.mkdir()
        (source / "a.py").write_text("a")
        cache = source / ".pytest_cache"
        cache.mkdir()
        (cache / "CACHEDIR.TAG").write_text("tag")

        zip_path = tmp_path / "out.zip"

        with patch.object(module_zipper, 'log'):
            assert module_zipper._zip_module(str(source), str(zip_path)) is True

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
        assert 'modules/a.py' in names
        assert not any('.pytest_cache' in n for n in names)


class TestZipModuleInternalGuard:
    """Tests for the zip-inside-source guard (lines 21-23)."""

    def test_zip_path_inside_source_raises_caught(self, tmp_path):
        """Returns False + logs error when zip_path lives inside source_path."""
        source = tmp_path / "modules"
        source.mkdir()
        (source / "a.py").write_text("a")
        # Zip path inside source_path
        zip_path = source / "inner.zip"

        with patch.object(module_zipper, 'log') as mock_log:
            result = module_zipper._zip_module(str(source), str(zip_path))

        assert result is False
        mock_log.error.assert_called_once()
        msg = mock_log.error.call_args.args[0]
        assert "Error zipping" in msg
        # The ValueError carries the guard message
        assert "must not be inside" in msg


class TestZipModuleInternalFailure:
    """Tests for exception handling (lines 50-52)."""

    def test_zipfile_open_failure_returns_false(self, tmp_path):
        """If ZipFile() raises, the broad except returns False and logs error."""
        source = tmp_path / "modules"
        source.mkdir()
        (source / "a.py").write_text("a")
        zip_path = tmp_path / "out.zip"

        boom = OSError("disk full")
        with patch.object(module_zipper.zipfile, 'ZipFile', side_effect=boom), \
             patch.object(module_zipper, 'log') as mock_log:
            result = module_zipper._zip_module(str(source), str(zip_path))

        assert result is False
        mock_log.error.assert_called_once()
        assert "disk full" in mock_log.error.call_args.args[0]

    def test_oswalk_failure_returns_false(self, tmp_path):
        """Failure mid-walk: source_path doesn't exist → walk yields nothing
        but writestr for parent dir still works; force a write failure to
        exercise the broad except clause."""
        source = tmp_path / "modules"
        source.mkdir()
        (source / "a.py").write_text("a")
        zip_path = tmp_path / "out.zip"

        # Make zipf.write blow up after the parent-dir writestr succeeds
        original_zipfile = module_zipper.zipfile.ZipFile

        class BadZip:
            def __init__(self, *a, **kw):
                self._real = original_zipfile(*a, **kw)

            def __enter__(self):
                self._real.__enter__()
                return self

            def __exit__(self, *exc):
                return self._real.__exit__(*exc)

            def writestr(self, *a, **kw):
                return self._real.writestr(*a, **kw)

            def write(self, *a, **kw):
                raise RuntimeError("write failed")

        with patch.object(module_zipper.zipfile, 'ZipFile', BadZip), \
             patch.object(module_zipper, 'log') as mock_log:
            result = module_zipper._zip_module(str(source), str(zip_path))

        assert result is False
        mock_log.error.assert_called_once()
        assert "write failed" in mock_log.error.call_args.args[0]
