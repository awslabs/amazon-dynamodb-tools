"""Pick one consumed-capacity series out of the lake's rows."""

PREFERRED_PERIOD_SECONDS = 60


def table_level_sum_series(df, metric_name):
    """Return the table-level Sum rows of one metric at a single period.

    The lake holds the same traffic several times: at each configured period
    (60s and 300s) and, for per-operation metrics, once per operation next to
    the table-level row. Summing or pairing all of them counts it twice.
    Keep rows without an operation, then one period: 60s (the autoscaling
    simulation granularity) when present, else the finest period available.
    """
    rows = df[(df["metric_name"] == metric_name) & (df["statistic"] == "Sum")]
    if "operation" in rows.columns:
        rows = rows[rows["operation"].isna()]
    if rows.empty:
        return rows
    periods = rows["period_seconds"]
    period = (
        PREFERRED_PERIOD_SECONDS
        if (periods == PREFERRED_PERIOD_SECONDS).any()
        else periods.min()
    )
    return rows[periods == period]
