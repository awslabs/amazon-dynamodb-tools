"""Unit tests for IncrementalExportParser."""

import unittest
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from python_modules.ddb_import.parsers.incremental_export_parser import IncrementalExportParser


class TestIncrementalExportParserNewAndOldImages(unittest.TestCase):
    """Test cases for IncrementalExportParser with NEW_AND_OLD_IMAGES view."""

    def setUp(self):
        self.parser = IncrementalExportParser(output_view="NEW_AND_OLD_IMAGES")

    def test_parse_insert_operation(self):
        """Test parsing an INSERT operation (NewImage only)."""
        record = {
            "Keys": {"id": {"N": "123"}},
            "NewImage": {"id": {"N": "123"}, "name": {"S": "John Doe"}},
            "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}
        }
        operation, data, condition = self.parser.parse_export_line(json.dumps(record))
        self.assertEqual(operation, "PUT")
        self.assertEqual(data, {"id": 123, "name": "John Doe"})
        self.assertEqual(condition, "attribute_not_exists(id)")

    def test_parse_modify_operation(self):
        """Test parsing a MODIFY operation (both OldImage and NewImage)."""
        record = {
            "Keys": {"id": {"N": "123"}},
            "OldImage": {"id": {"N": "123"}, "name": {"S": "John"}},
            "NewImage": {"id": {"N": "123"}, "name": {"S": "John Doe"}},
            "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}
        }
        operation, data, condition = self.parser.parse_export_line(json.dumps(record))
        self.assertEqual(operation, "PUT")
        self.assertEqual(data, {"id": 123, "name": "John Doe"})
        self.assertEqual(condition, "attribute_exists(id)")

    def test_parse_remove_operation(self):
        """Test parsing a REMOVE operation (OldImage only)."""
        record = {
            "Keys": {"id": {"N": "123"}},
            "OldImage": {"id": {"N": "123"}, "name": {"S": "John Doe"}},
            "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}
        }
        operation, data, condition = self.parser.parse_export_line(json.dumps(record))
        self.assertEqual(operation, "DELETE")
        self.assertEqual(data, {"id": 123})
        self.assertEqual(condition, "attribute_exists(id)")

    def test_parse_composite_key(self):
        """Test parsing with composite primary key."""
        record = {
            "Keys": {"pk": {"S": "USER"}, "sk": {"S": "123"}},
            "NewImage": {"pk": {"S": "USER"}, "sk": {"S": "123"}, "name": {"S": "John"}},
            "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}
        }
        operation, data, condition = self.parser.parse_export_line(json.dumps(record))
        self.assertEqual(operation, "PUT")
        self.assertEqual(data, {"pk": "USER", "sk": "123", "name": "John"})
        self.assertEqual(condition, "attribute_not_exists(pk)")

    def test_parse_invalid_record_no_images(self):
        """Test parsing record with neither OldImage nor NewImage."""
        record = {
            "Keys": {"id": {"N": "123"}},
            "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}
        }
        with self.assertRaises(ValueError) as context:
            self.parser.parse_export_line(json.dumps(record))
        self.assertIn("no OldImage or NewImage", str(context.exception))

    def test_parse_malformed_json(self):
        """Test parsing malformed JSON."""
        with self.assertRaises(ValueError) as context:
            self.parser.parse_export_line("invalid json")
        self.assertIn("Malformed JSON", str(context.exception))

    def test_parse_missing_keys(self):
        """Test parsing record missing Keys field."""
        record = {"NewImage": {"id": {"N": "123"}}, "Metadata": {"WriteTimestampMicros": {"N": "1"}}}
        with self.assertRaises(ValueError) as context:
            self.parser.parse_export_line(json.dumps(record))
        self.assertIn("missing 'Keys' field", str(context.exception))

    def test_parse_missing_metadata(self):
        """Test parsing record missing Metadata field."""
        record = {"Keys": {"id": {"N": "123"}}, "NewImage": {"id": {"N": "123"}}}
        with self.assertRaises(ValueError) as context:
            self.parser.parse_export_line(json.dumps(record))
        self.assertIn("missing 'Metadata' field", str(context.exception))

    def test_parse_non_dict_json(self):
        """Test parsing JSON that's not a dictionary."""
        with self.assertRaises(ValueError) as context:
            self.parser.parse_export_line('["not", "a", "dict"]')
        self.assertIn("must be a JSON object", str(context.exception))


class TestIncrementalExportParserNewImage(unittest.TestCase):
    """Test cases for IncrementalExportParser with NEW_IMAGE view."""

    def setUp(self):
        self.parser = IncrementalExportParser(output_view="NEW_IMAGE")

    def test_parse_insert_or_modify_returns_unconditional_put(self):
        """Test that NewImage-only record returns unconditional PUT (no condition)."""
        record = {
            "Keys": {"id": {"N": "123"}},
            "NewImage": {"id": {"N": "123"}, "name": {"S": "John Doe"}},
            "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}
        }
        operation, data, condition = self.parser.parse_export_line(json.dumps(record))
        self.assertEqual(operation, "PUT")
        self.assertEqual(data, {"id": 123, "name": "John Doe"})
        self.assertIsNone(condition)

    def test_parse_remove_returns_conditional_delete(self):
        """Test that Keys-only record (REMOVE) returns conditional DELETE."""
        record = {
            "Keys": {"id": {"N": "123"}},
            "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}
        }
        operation, data, condition = self.parser.parse_export_line(json.dumps(record))
        self.assertEqual(operation, "DELETE")
        self.assertEqual(data, {"id": 123})
        self.assertEqual(condition, "attribute_exists(id)")

    def test_parse_composite_key_unconditional_put(self):
        """Test composite key with NEW_IMAGE returns unconditional PUT."""
        record = {
            "Keys": {"pk": {"S": "USER"}, "sk": {"S": "123"}},
            "NewImage": {"pk": {"S": "USER"}, "sk": {"S": "123"}, "val": {"N": "42"}},
            "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}
        }
        operation, data, condition = self.parser.parse_export_line(json.dumps(record))
        self.assertEqual(operation, "PUT")
        self.assertEqual(data, {"pk": "USER", "sk": "123", "val": 42})
        self.assertIsNone(condition)

    def test_real_world_new_image_record(self):
        """Test parsing a real-world NEW_IMAGE export record."""
        record = {
            "Metadata": {"WriteTimestampMicros": {"N": "1774379199746598"}},
            "Keys": {"timestamp": {"S": "2026-03-11T10:03:00Z"}, "turbine_id": {"S": "new_item2"}},
            "NewImage": {"timestamp": {"S": "2026-03-11T10:03:00Z"}, "turbine_id": {"S": "new_item2"}}
        }
        operation, data, condition = self.parser.parse_export_line(json.dumps(record))
        self.assertEqual(operation, "PUT")
        self.assertEqual(data, {"timestamp": "2026-03-11T10:03:00Z", "turbine_id": "new_item2"})
        self.assertIsNone(condition)


class TestIncrementalExportParserInit(unittest.TestCase):
    """Test IncrementalExportParser initialization."""

    def test_default_output_view_is_new_and_old_images(self):
        parser = IncrementalExportParser()
        self.assertEqual(parser.output_view, "NEW_AND_OLD_IMAGES")

    def test_invalid_output_view_raises(self):
        with self.assertRaises(ValueError):
            IncrementalExportParser(output_view="INVALID")


if __name__ == '__main__':
    unittest.main()
