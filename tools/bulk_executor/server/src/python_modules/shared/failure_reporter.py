"""Report per-item failures from a Spark worker without flooding CloudWatch.

Worker output has an awkward property: **nobody reads it live.** The client subscribes
to the driver stream and every `<job_run_id>_g-*` executor stream, but
`_pretty_print_log_event` discards executor events outright, so a `print()` inside
`foreachPartition` / `rdd.map` never reaches the person running the command. It is
still worth emitting -- it lands in CloudWatch, which is where a failed run gets
diagnosed afterwards -- but only in bounded quantity, and the user-facing count has to
come from the driver.

Unbounded is not a theoretical concern. `find`'s delete path printed one line per
failed item, interpolating the whole item, across 200 partitions: a systemic failure on
a large table is up to **800 MB** of log nobody reads. Two caps are needed and they are
independent:

- **count**, per partition -- the multiplier is the partition count (200 for `find`
  deletes, 800 workers for `update`), so "first 10" is really 2,000 or 8,000 lines;
- **line size** -- log the key, not the item. Ten lines of a 400 KB item is worse than
  ten thousand lines of a key.

See `ai_lint/rules/console_output_rate.md` for the policy this implements.
"""

# First N failures per partition. Small enough that 200 or 800 partitions stay in the
# hundreds of KB, large enough to show a pattern rather than a single unlucky item.
MAX_REPORTED_PER_PARTITION = 10


class BoundedFailureReporter:
    """Counts every failure, prints the first few, says once that it stopped.

    One instance per partition -- the cap is per partition, and the instance carries the
    running count. The total reaches the user through `accumulator`, which the driver
    reads after the job to print a summary; that summary is the only part the user
    actually sees.
    """

    def __init__(self, label, accumulator=None,
                 max_reported=MAX_REPORTED_PER_PARTITION, emit=print):
        self.label = label
        self.accumulator = accumulator
        self.max_reported = max_reported
        self.emit = emit
        self.count = 0

    def report(self, identifier, error):
        """Record one failure. `identifier` should be a key, never a whole item."""
        self.count += 1
        if self.accumulator is not None:
            self.accumulator.add(1)

        if self.count <= self.max_reported:
            self.emit(f"{self.label} failed for {identifier}: {error}")
        elif self.count == self.max_reported + 1:
            self.emit(
                f"More {self.label} failures in this partition; only the first "
                f"{self.max_reported} are logged. The run total is reported at the end.")
