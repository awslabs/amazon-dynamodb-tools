# ddb-metrics-collection

This project was built with poetry and you can execute with the following command:

```
❯ poetry run python -m ddb_metrics_collection.main --start-time 2025-01-01
Collecting metrics from 2025-01-01 to 2025-02-14T23:59:59.999999+00:00
Collecting metrics from 2025-01-01 00:00:00+00:00 to 2025-02-14 23:59:59.999999+00:00
Collecting metrics: 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████| 4/4 [00:15<00:00,  3.82s/it]
Metrics collected and stored successfully.

Utilization data written to: dynamodb_utilization_20250214_173905.csv

Total tables with low utilization (0 <= utilization <= 0.45): 4
```

By default the output file `dynamodb_utilization_YYYYMMDD_HHMMSS.csv` 

You can also execute the python package by installing the `requirements.txt` file independently.