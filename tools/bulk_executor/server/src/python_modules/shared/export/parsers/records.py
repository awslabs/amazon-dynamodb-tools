class FullExportRecord:
    def __init__(self, item, table_key_schema):
        self.item = item
        self.table_key_schema = table_key_schema


class IncrementalExportRecord:
    def __init__(self, keys, new_image, old_image, table_key_schema, write_timestamp_micros):
        self.keys = keys
        self.new_image = new_image
        self.old_image = old_image
        self.table_key_schema = table_key_schema
        self.write_timestamp_micros = write_timestamp_micros
