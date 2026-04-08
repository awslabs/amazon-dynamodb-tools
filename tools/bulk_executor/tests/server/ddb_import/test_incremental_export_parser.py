"""Unit tests for IncrementalExportParser."""

import unittest
import json

from python_modules.ddb_import.parsers.incremental_export_parser import IncrementalExportParser


class TestIncrementalExportParserPut(unittest.TestCase):
    """Test cases for PUT operations (NewImage present)."""

    def setUp(self):
        self.parser = IncrementalExportParser()

    def test_parse_insert_returns_unconditional_put(self):
        """NewImage only (insert) returns unconditional PUT."""
        record = {
            "Keys": {"id": {"N": "123"}},
            "NewImage": {"id": {"N": "123"}, "name": {"S": "John Doe"}},
            "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}
        }
        operation, data = self.parser.parse_export_line(json.dumps(record))
        self.assertEqual(operation, "PUT")
        self.assertEqual(data, {"id": 123, "name": "John Doe"})

    def test_parse_modify_returns_unconditional_put(self):
        """Both OldImage and NewImage (modify) returns unconditional PUT with new data."""
        record = {
            "Keys": {"id": {"N": "123"}},
            "OldImage": {"id": {"N": "123"}, "name": {"S": "John"}},
            "NewImage": {"id": {"N": "123"}, "name": {"S": "John Doe"}},
            "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}
        }
        operation, data = self.parser.parse_export_line(json.dumps(record))
        self.assertEqual(operation, "PUT")
        self.assertEqual(data, {"id": 123, "name": "John Doe"})

    def test_parse_composite_key_put(self):
        """Composite key with NewImage returns PUT."""
        record = {
            "Keys": {"pk": {"S": "USER"}, "sk": {"S": "123"}},
            "NewImage": {"pk": {"S": "USER"}, "sk": {"S": "123"}, "name": {"S": "John"}},
            "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}
        }
        operation, data = self.parser.parse_export_line(json.dumps(record))
        self.assertEqual(operation, "PUT")
        self.assertEqual(data, {"pk": "USER", "sk": "123", "name": "John"})


class TestIncrementalExportParserDelete(unittest.TestCase):
    """Test cases for DELETE operations (no NewImage)."""

    def setUp(self):
        self.parser = IncrementalExportParser()

    def test_parse_remove_with_old_image(self):
        """OldImage only (remove) returns unconditional DELETE with keys."""
        record = {
            "Keys": {"id": {"N": "123"}},
            "OldImage": {"id": {"N": "123"}, "name": {"S": "John Doe"}},
            "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}
        }
        operation, data = self.parser.parse_export_line(json.dumps(record))
        self.assertEqual(operation, "DELETE")
        self.assertEqual(data, {"id": 123})

    def test_parse_remove_with_null_new_image(self):
        """NewImage explicitly null (NEW_IMAGE view delete) returns DELETE."""
        record = {
            "Keys": {"id": {"N": "123"}},
            "NewImage": None,
            "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}
        }
        operation, data = self.parser.parse_export_line(json.dumps(record))
        self.assertEqual(operation, "DELETE")
        self.assertEqual(data, {"id": 123})

    def test_parse_remove_without_new_image_key(self):
        """No NewImage key at all returns DELETE."""
        record = {
            "Keys": {"id": {"N": "123"}},
            "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}
        }
        operation, data = self.parser.parse_export_line(json.dumps(record))
        self.assertEqual(operation, "DELETE")
        self.assertEqual(data, {"id": 123})


class TestIncrementalExportParserValidation(unittest.TestCase):
    """Test cases for input validation."""

    def setUp(self):
        self.parser = IncrementalExportParser()

    def test_parse_malformed_json(self):
        with self.assertRaises(ValueError) as context:
            self.parser.parse_export_line("invalid json")
        self.assertIn("Malformed JSON", str(context.exception))

    def test_parse_missing_keys(self):
        record = {"NewImage": {"id": {"N": "123"}}, "Metadata": {"WriteTimestampMicros": {"N": "1"}}}
        with self.assertRaises(ValueError) as context:
            self.parser.parse_export_line(json.dumps(record))
        self.assertIn("missing 'Keys' field", str(context.exception))

    def test_parse_missing_metadata(self):
        record = {"Keys": {"id": {"N": "123"}}, "NewImage": {"id": {"N": "123"}}}
        with self.assertRaises(ValueError) as context:
            self.parser.parse_export_line(json.dumps(record))
        self.assertIn("missing 'Metadata' field", str(context.exception))

    def test_parse_non_dict_json(self):
        with self.assertRaises(ValueError) as context:
            self.parser.parse_export_line('["not", "a", "dict"]')
        self.assertIn("must be a JSON object", str(context.exception))

    def test_real_world_new_image_record(self):
        """Test parsing a real-world export record."""
        record = {
            "Metadata": {"WriteTimestampMicros": {"N": "1774379199746598"}},
            "Keys": {"timestamp": {"S": "2026-03-11T10:03:00Z"}, "turbine_id": {"S": "new_item2"}},
            "NewImage": {"timestamp": {"S": "2026-03-11T10:03:00Z"}, "turbine_id": {"S": "new_item2"}}
        }
        operation, data = self.parser.parse_export_line(json.dumps(record))
        self.assertEqual(operation, "PUT")
        self.assertEqual(data, {"timestamp": "2026-03-11T10:03:00Z", "turbine_id": "new_item2"})


if __name__ == '__main__':
    unittest.main()
