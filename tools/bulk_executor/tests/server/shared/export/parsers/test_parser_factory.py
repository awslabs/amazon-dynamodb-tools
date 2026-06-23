"""Unit tests for ParserFactory."""

import pytest
from python_modules.shared.export.parsers.parser_factory import ParserFactory
from python_modules.shared.export.parsers.full_export_parser import FullExportParser
from python_modules.shared.export.parsers.incremental_export_parser import IncrementalExportParser
from python_modules.shared.export.utils.enums import ExportLoadType


class TestParserFactory:

    def test_full_export_returns_full_parser(self):
        key_schema = {'pk': {'name': 'id', 'type': 'S'}}
        parser = ParserFactory.get_parser(ExportLoadType.FULL, key_schema)
        assert isinstance(parser, FullExportParser)

    def test_incremental_export_returns_incremental_parser(self):
        key_schema = {'pk': {'name': 'id', 'type': 'S'}}
        parser = ParserFactory.get_parser(ExportLoadType.INCREMENTAL, key_schema)
        assert isinstance(parser, IncrementalExportParser)

    def test_unsupported_load_type_raises_value_error(self):
        key_schema = {'pk': {'name': 'id', 'type': 'S'}}
        with pytest.raises(ValueError, match="Unsupported load type"):
            ParserFactory.get_parser("UNKNOWN_TYPE", key_schema)

    def test_unsupported_load_type_includes_type_in_message(self):
        key_schema = {'pk': {'name': 'id', 'type': 'S'}}
        with pytest.raises(ValueError, match="BOGUS"):
            ParserFactory.get_parser("BOGUS", key_schema)
