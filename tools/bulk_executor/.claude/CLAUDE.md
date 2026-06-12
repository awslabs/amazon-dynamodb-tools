# Skills

## Run Unit Tests

After completing a logical set of changes, run the unit tests to verify nothing is broken.

All python commands must activate the local virtual environment first.

### Steps

From the `tools/bulk_executor/` directory:

```bash
source .venv/bin/activate && make install && make test
```

### When to run

- After completing a cohesive set of related changes (not after every single edit)
- Before reporting that a task is complete
- After refactoring or modifying existing functionality

### Notes

- Use `make test-client` or `make test-server` if changes are scoped to only one side
- If tests fail, fix the issues before moving on
