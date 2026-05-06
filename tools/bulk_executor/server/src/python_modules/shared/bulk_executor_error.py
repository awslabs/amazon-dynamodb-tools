class BulkExecutorError(Exception):
    """Expected errors from bad user input. Caught in root.py to fail cleanly without Glue stack traces."""
    pass
