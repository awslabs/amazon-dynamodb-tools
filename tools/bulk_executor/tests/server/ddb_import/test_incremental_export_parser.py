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
        operation, data, condition, expr_names = self.parser.parse_export_line(json.dumps(record))
        self.assertEqual(operation, "PUT")
        self.assertEqual(data, {"id": 123, "name": "John Doe"})
        self.assertIn("attribute_not_exists(", condition)
        self.assertIn("id", expr_names.values())

    def test_parse_modify_operation(self):
        """Test parsing a MODIFY operation (both OldImage and NewImage)."""
        record = {
            "Keys": {"id": {"N": "123"}},
            "OldImage": {"id": {"N": "123"}, "name": {"S": "John"}},
            "NewImage": {"id": {"N": "123"}, "name": {"S": "John Doe"}},
            "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}
        }
        operation, data, condition, expr_names = self.parser.parse_export_line(json.dumps(record))
        self.assertEqual(operation, "PUT")
        self.assertEqual(data, {"id": 123, "name": "John Doe"})
        self.assertIn("attribute_exists(", condition)
        self.assertIn("id", expr_names.values())

    def test_parse_remove_operation(self):
        """Test parsing a REMOVE operation (OldImage only)."""
        record = {
            "Keys": {"id": {"N": "123"}},
            "OldImage": {"id": {"N": "123"}, "name": {"S": "John Doe"}},
            "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}
        }
        operation, data, condition, expr_names = self.parser.parse_export_line(json.dumps(record))
        self.assertEqual(operation, "DELETE")
        self.assertEqual(data, {"id": 123})
        self.assertIn("attribute_exists(", condition)
        self.assertIn("id", expr_names.values())

    def test_parse_composite_key(self):
        """Test parsing with composite primary key."""
        record = {
            "Keys": {"pk": {"S": "USER"}, "sk": {"S": "123"}},
            "NewImage": {"pk": {"S": "USER"}, "sk": {"S": "123"}, "name": {"S": "John"}},
            "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}
        }
        operation, data, condition, expr_names = self.parser.parse_export_line(json.dumps(record))
        self.assertEqual(operation, "PUT")
        self.assertEqual(data, {"pk": "USER", "sk": "123", "name": "John"})
        self.assertIn("attribute_not_exists(", condition)
        self.assertEqual(len(expr_names), 2)

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
        operation, data, condition, expr_names = self.parser.parse_export_line(json.dumps(record))
        self.assertEqual(operation, "PUT")
        self.assertEqual(data, {"id": 123, "name": "John Doe"})
        self.assertIsNone(condition)
        self.assertIsNone(expr_names)

    def test_parse_remove_returns_conditional_delete(self):
        """Test that Keys-only record (REMOVE) returns conditional DELETE."""
        record = {
            "Keys": {"id": {"N": "123"}},
            "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}
        }
        operation, data, condition, expr_names = self.parser.parse_export_line(json.dumps(record))
        self.assertEqual(operation, "DELETE")
        self.assertEqual(data, {"id": 123})
        self.assertIn("attribute_exists(", condition)
        self.assertIn("id", expr_names.values())

    def test_parse_composite_key_unconditional_put(self):
        """Test composite key with NEW_IMAGE returns unconditional PUT."""
        record = {
            "Keys": {"pk": {"S": "USER"}, "sk": {"S": "123"}},
            "NewImage": {"pk": {"S": "USER"}, "sk": {"S": "123"}, "val": {"N": "42"}},
            "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}
        }
        operation, data, condition, expr_names = self.parser.parse_export_line(json.dumps(record))
        self.assertEqual(operation, "PUT")
        self.assertEqual(data, {"pk": "USER", "sk": "123", "val": 42})
        self.assertIsNone(condition)
        self.assertIsNone(expr_names)

    def test_real_world_new_image_record(self):
        """Test parsing a real-world NEW_IMAGE export record."""
        record = {
            "Metadata": {"WriteTimestampMicros": {"N": "1774379199746598"}},
            "Keys": {"timestamp": {"S": "2026-03-11T10:03:00Z"}, "turbine_id": {"S": "new_item2"}},
            "NewImage": {"timestamp": {"S": "2026-03-11T10:03:00Z"}, "turbine_id": {"S": "new_item2"}}
        }
        operation, data, condition, expr_names = self.parser.parse_export_line(json.dumps(record))
        self.assertEqual(operation, "PUT")
        self.assertEqual(data, {"timestamp": "2026-03-11T10:03:00Z", "turbine_id": "new_item2"})
        self.assertIsNone(condition)
        self.assertIsNone(expr_names)


class TestIncrementalExportParserReservedKeywords(unittest.TestCase):
    """Test that condition expressions handle reserved keywords via ExpressionAttributeNames."""

    def test_reserved_keyword_pk_insert(self):
        """Reserved keyword 'timestamp' as key must use placeholder in condition."""
        parser = IncrementalExportParser(output_view="NEW_AND_OLD_IMAGES")
        record = {
            "Keys": {"timestamp": {"S": "2026-01-01"}, "turbine_id": {"S": "WT-001"}},
            "NewImage": {"timestamp": {"S": "2026-01-01"}, "turbine_id": {"S": "WT-001"}, "val": {"N": "42"}},
            "Metadata": {"WriteTimestampMicros": {"N": "123"}}
        }
        result = parser.parse_export_line(json.dumps(record))
        operation, data, condition, expr_names = result
        self.assertEqual(operation, "PUT")
        # Condition must NOT contain raw 'timestamp' — must use placeholders
        self.assertNotIn("timestamp", condition)
        self.assertIn("attribute_not_exists(", condition)
        # ExpressionAttributeNames must map placeholders to actual key names
        self.assertIn("timestamp", expr_names.values())
        self.assertIn("turbine_id", expr_names.values())

    def test_reserved_keyword_pk_modify(self):
        """MODIFY with reserved keyword keys uses placeholders."""
        parser = IncrementalExportParser(output_view="NEW_AND_OLD_IMAGES")
        record = {
            "Keys": {"timestamp": {"S": "2026-01-01"}, "turbine_id": {"S": "WT-001"}},
            "OldImage": {"timestamp": {"S": "2026-01-01"}, "turbine_id": {"S": "WT-001"}, "val": {"N": "1"}},
            "NewImage": {"timestamp": {"S": "2026-01-01"}, "turbine_id": {"S": "WT-001"}, "val": {"N": "2"}},
            "Metadata": {"WriteTimestampMicros": {"N": "123"}}
        }
        result = parser.parse_export_line(json.dumps(record))
        operation, data, condition, expr_names = result
        self.assertEqual(operation, "PUT")
        self.assertNotIn("timestamp", condition)
        self.assertIn("attribute_exists(", condition)
        self.assertIn("timestamp", expr_names.values())
        self.assertIn("turbine_id", expr_names.values())

    def test_reserved_keyword_pk_delete(self):
        """DELETE with reserved keyword keys uses placeholders."""
        parser = IncrementalExportParser(output_view="NEW_AND_OLD_IMAGES")
        record = {
            "Keys": {"timestamp": {"S": "2026-01-01"}, "turbine_id": {"S": "WT-001"}},
            "OldImage": {"timestamp": {"S": "2026-01-01"}, "turbine_id": {"S": "WT-001"}, "val": {"N": "1"}},
            "Metadata": {"WriteTimestampMicros": {"N": "123"}}
        }
        result = parser.parse_export_line(json.dumps(record))
        operation, data, condition, expr_names = result
        self.assertEqual(operation, "DELETE")
        self.assertNotIn("timestamp", condition)
        self.assertIn("attribute_exists(", condition)
        self.assertIn("timestamp", expr_names.values())
        self.assertIn("turbine_id", expr_names.values())

    def test_composite_key_condition_checks_both_keys(self):
        """Condition must check both PK and SK, not just one."""
        parser = IncrementalExportParser(output_view="NEW_AND_OLD_IMAGES")
        record = {
            "Keys": {"pk": {"S": "A"}, "sk": {"S": "B"}},
            "NewImage": {"pk": {"S": "A"}, "sk": {"S": "B"}},
            "Metadata": {"WriteTimestampMicros": {"N": "123"}}
        }
        result = parser.parse_export_line(json.dumps(record))
        operation, data, condition, expr_names = result
        # Must have two attribute_not_exists checks joined by AND
        self.assertEqual(condition.count("attribute_not_exists("), 2)
        self.assertIn(" AND ", condition)
        self.assertEqual(len(expr_names), 2)

    def test_new_image_delete_reserved_keyword(self):
        """NEW_IMAGE view DELETE with reserved keyword uses placeholders."""
        parser = IncrementalExportParser(output_view="NEW_IMAGE")
        record = {
            "Keys": {"timestamp": {"S": "2026-01-01"}, "turbine_id": {"S": "WT-001"}},
            "Metadata": {"WriteTimestampMicros": {"N": "123"}}
        }
        result = parser.parse_export_line(json.dumps(record))
        operation, data, condition, expr_names = result
        self.assertEqual(operation, "DELETE")
        self.assertNotIn("timestamp", condition)
        self.assertIn("timestamp", expr_names.values())

    def test_new_image_put_returns_no_expr_names(self):
        """NEW_IMAGE view PUT has no condition, so expr_names should be None."""
        parser = IncrementalExportParser(output_view="NEW_IMAGE")
        record = {
            "Keys": {"timestamp": {"S": "2026-01-01"}},
            "NewImage": {"timestamp": {"S": "2026-01-01"}, "val": {"N": "1"}},
            "Metadata": {"WriteTimestampMicros": {"N": "123"}}
        }
        result = parser.parse_export_line(json.dumps(record))
        operation, data, condition, expr_names = result
        self.assertEqual(operation, "PUT")
        self.assertIsNone(condition)
        self.assertIsNone(expr_names)


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
