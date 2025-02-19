import json
import os
import numpy as np
from datetime import datetime


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class MetricsStorage:
    def __init__(self, storage_type="disk", base_path="metrics"):
        self.storage_type = storage_type
        self.base_path = base_path
        self.memory_storage = {}

    async def store(self, metrics):
        if self.storage_type == "disk":
            os.makedirs(self.base_path, exist_ok=True)
            for region, region_metrics in metrics.items():
                region_path = os.path.join(self.base_path, region)
                os.makedirs(region_path, exist_ok=True)
                for table, table_metrics in region_metrics.items():
                    table_file = os.path.join(region_path, f"{table}.npy")
                    self._store_table_metrics(table_metrics, table_file)
        elif self.storage_type == "memory":
            self.memory_storage = metrics

    def _store_table_metrics(self, table_metrics, file_path):
        if not table_metrics:
            print(f"No metrics data for {file_path}")
            return

        # Determine the metrics available in the data
        available_metrics = set()
        for entry in table_metrics:
            available_metrics.update(entry["Metrics"].keys())

        # Create the dtype based on available metrics
        dtype = [("timestamp", "datetime64[ns]")]
        for metric in available_metrics:
            dtype.extend([(f"{metric}_Average", "f8"), (f"{metric}_Maximum", "f8")])

        data = []
        for entry in table_metrics:
            row = [np.datetime64(entry["Timestamp"])]
            for metric in available_metrics:
                values = entry["Metrics"].get(metric, {"Average": 0, "Maximum": 0})
                row.extend([values["Average"], values["Maximum"]])
            data.append(tuple(row))

        try:
            arr = np.array(data, dtype=dtype)
            np.save(file_path, arr)
            print(f"Saved metrics to {file_path}")
        except Exception as e:
            print(f"Error saving metrics to {file_path}: {str(e)}")
            print(f"Data shape: {len(data)} rows, {len(dtype)} columns")
            print(f"First row: {data[0] if data else 'No data'}")
            print(f"dtype: {dtype}")

    async def retrieve(self, region, table):
        if self.storage_type == "disk":
            file_path = os.path.join(self.base_path, region, f"{table}.npy")
            if os.path.exists(file_path):
                return np.load(file_path)
        elif self.storage_type == "memory":
            return self.memory_storage.get(region, {}).get(table)
        return None
