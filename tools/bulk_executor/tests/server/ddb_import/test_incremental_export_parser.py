"""Unit tests for IncrementalExportParser."""

import unittest
import json
from python_modules.ddb_import.parsers.incremental_export_parser import IncrementalExportParser

SAMPLE_KEY_SCHEMA = {'pk': {'name': 'id', 'type': 'N'}}
COMPOSITE_KEY_SCHEMA = {'pk': {'name': 'pk', 'type': 'S'}, 'sk': {'name': 'sk', 'type': 'S'}}


class TestIncrementalExportParserPut(unittest.TestCase):

    def setUp(self):
        self.parser = IncrementalExportParser(SAMPLE_KEY_SCHEMA)

    def test_parse_to_record_insert(self):
        record_data = {"Keys": {"id": {"N": "123"}}, "NewImage": {"id": {"N": "123"}, "name": {"S": "John"}}, "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}}
        record = self.parser.parse_to_record(json.dumps(record_data))
        self.assertEqual(record.keys, {"id": 123})
        self.assertEqual(record.new_image, {"id": 123, "name": "John"})
        self.assertIsNone(record.old_image)
        self.assertEqual(record.write_timestamp_micros, "1234567890")

    def test_resolve_insert(self):
        record_data = {"Keys": {"id": {"N": "123"}}, "NewImage": {"id": {"N": "123"}, "name": {"S": "John Doe"}}, "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}}
        record = self.parser.parse_to_record(json.dumps(record_data))
        result = self.parser.resolve(record)
        self.assertEqual(result["operation"], "PUT")
        self.assertEqual(result["data"], {"id": 123, "name": "John Doe"})

    def test_resolve_modify(self):
        record_data = {"Keys": {"id": {"N": "123"}}, "OldImage": {"id": {"N": "123"}, "name": {"S": "John"}}, "NewImage": {"id": {"N": "123"}, "name": {"S": "John Doe"}}, "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}}
        record = self.parser.parse_to_record(json.dumps(record_data))
        result = self.parser.resolve(record)
        self.assertEqual(result["operation"], "PUT")
        self.assertEqual(result["data"], {"id": 123, "name": "John Doe"})

    def test_composite_key(self):
        parser = IncrementalExportParser(COMPOSITE_KEY_SCHEMA)
        record_data = {"Keys": {"pk": {"S": "USER"}, "sk": {"S": "123"}}, "NewImage": {"pk": {"S": "USER"}, "sk": {"S": "123"}, "name": {"S": "John"}}, "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}}
        record = parser.parse_to_record(json.dumps(record_data))
        result = parser.resolve(record)
        self.assertEqual(result["operation"], "PUT")
        self.assertEqual(result["data"], {"pk": "USER", "sk": "123", "name": "John"})


class TestIncrementalExportParserDelete(unittest.TestCase):

    def setUp(self):
        self.parser = IncrementalExportParser(SAMPLE_KEY_SCHEMA)

    def test_remove_with_old_image(self):
        record_data = {"Keys": {"id": {"N": "123"}}, "OldImage": {"id": {"N": "123"}, "name": {"S": "John"}}, "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}}
        record = self.parser.parse_to_record(json.dumps(record_data))
        result = self.parser.resolve(record)
        self.assertEqual(result["operation"], "DELETE")
        self.assertEqual(result["data"], {"id": 123})

    def test_remove_with_null_new_image(self):
        record_data = {"Keys": {"id": {"N": "123"}}, "NewImage": None, "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}}
        record = self.parser.parse_to_record(json.dumps(record_data))
        result = self.parser.resolve(record)
        self.assertEqual(result["operation"], "DELETE")

    def test_remove_without_new_image_key(self):
        record_data = {"Keys": {"id": {"N": "123"}}, "Metadata": {"WriteTimestampMicros": {"N": "1234567890"}}}
        record = self.parser.parse_to_record(json.dumps(record_data))
        result = self.parser.resolve(record)
        self.assertEqual(result["operation"], "DELETE")


class TestIncrementalExportParserValidation(unittest.TestCase):

    def setUp(self):
        self.parser = IncrementalExportParser(SAMPLE_KEY_SCHEMA)

    def test_malformed_json(self):
        with self.assertRaises(ValueError):
            self.parser.parse_to_record("invalid json")

    def test_missing_keys(self):
        with self.assertRaises(ValueError):
            self.parser.parse_to_record(json.dumps({"NewImage": {"id": {"N": "123"}}, "Metadata": {"WriteTimestampMicros": {"N": "1"}}}))

    def test_missing_metadata(self):
        with self.assertRaises(ValueError):
            self.parser.parse_to_record(json.dumps({"Keys": {"id": {"N": "123"}}, "NewImage": {"id": {"N": "123"}}}))

    def test_non_dict_json(self):
        with self.assertRaises(ValueError):
            self.parser.parse_to_record('["not", "a", "dict"]')


if __name__ == '__main__':
    unittest.main()
