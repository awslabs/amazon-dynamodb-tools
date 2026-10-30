"""
Named per-region DynamoDB prices from the pricing_data table.

The PricingCollector stores raw AWS Price List rows; this module turns them into
named prices (e.g. ``provisioned_write`` in USD per WCU-hour) with strict rules:

- Usage types are matched exactly, never by substring. Regions differ in how
  they prefix usage types: us-east-1 leaves throughput/storage unprefixed
  (``ReadRequestUnits``, ``IA-ReadRequestUnits``) but prefixes the rest
  (``USE1-ExportDataSize-Bytes``); other regions prefix everything
  (``EU-ReadRequestUnits``). Both forms are accepted for every price.
- Only the paid tier is returned (``end_range = 'Inf'``), never a free tier.
- A price that is not in the data is omitted, never defaulted; asking for it in
  ``required`` raises MissingPricingError.
"""

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Mapping, NamedTuple, Optional, Tuple

ON_DEMAND = "OnDemand"
RESERVED = "Reserved"
UPFRONT_UNIT = "Quantity"


class PriceSpec(NamedTuple):
    """How to find one named price in pricing_data."""

    usage_type: str  # Without the region prefix, e.g. "IA-WriteCapacityUnit-Hrs"
    term_type: str = ON_DEMAND
    lease: Optional[str] = None  # "1yr" / "3yr" for Reserved terms
    upfront: bool = (
        False  # Reserved upfront fee (unit "Quantity") instead of hourly rate
    )


def _reserved(lease: str) -> Dict[str, PriceSpec]:
    return {
        f"reserved_{lease}_read_hourly": PriceSpec(
            "ReadCapacityUnit-Hrs", RESERVED, lease
        ),
        f"reserved_{lease}_read_upfront": PriceSpec(
            "ReadCapacityUnit-Hrs", RESERVED, lease, True
        ),
        f"reserved_{lease}_write_hourly": PriceSpec(
            "WriteCapacityUnit-Hrs", RESERVED, lease
        ),
        f"reserved_{lease}_write_upfront": PriceSpec(
            "WriteCapacityUnit-Hrs", RESERVED, lease, True
        ),
    }


# Every named price, with its unit as published by AWS.
PRICE_KEYS: Dict[str, PriceSpec] = {
    # On-demand request units (USD per request unit)
    "on_demand_read": PriceSpec("ReadRequestUnits"),
    "on_demand_write": PriceSpec("WriteRequestUnits"),
    "on_demand_read_ia": PriceSpec("IA-ReadRequestUnits"),
    "on_demand_write_ia": PriceSpec("IA-WriteRequestUnits"),
    # Provisioned capacity (USD per capacity-unit hour)
    "provisioned_read": PriceSpec("ReadCapacityUnit-Hrs"),
    "provisioned_write": PriceSpec("WriteCapacityUnit-Hrs"),
    "provisioned_read_ia": PriceSpec("IA-ReadCapacityUnit-Hrs"),
    "provisioned_write_ia": PriceSpec("IA-WriteCapacityUnit-Hrs"),
    # Reserved capacity
    # (hourly: USD per capacity-unit hour; upfront: USD per capacity unit)
    **_reserved("1yr"),
    **_reserved("3yr"),
    # Storage (USD per GB-month)
    "storage": PriceSpec("TimedStorage-ByteHrs"),
    "storage_ia": PriceSpec("IA-TimedStorage-ByteHrs"),
    # Global tables replicated writes
    "replicated_write_on_demand": PriceSpec("ReplWriteRequestUnits"),
    "replicated_write_on_demand_ia": PriceSpec("IA-ReplWriteRequestUnits"),
    "replicated_write_provisioned": PriceSpec("ReplWriteCapacityUnit-Hrs"),
    "replicated_write_provisioned_ia": PriceSpec("IA-ReplWriteCapacityUnit-Hrs"),
    # Data movement (USD per GB)
    "export_full": PriceSpec("ExportDataSize-Bytes"),
    "export_incremental": PriceSpec("IncrementalExportDataSize-Bytes"),
    "import": PriceSpec("ImportDataSize-Bytes"),
    "restore": PriceSpec("RestoreDataSize-Bytes"),
    # Backups (USD per GB-month)
    "backup_storage": PriceSpec("TimedBackupStorage-ByteHrs"),
    "pitr_storage": PriceSpec("TimedPITRStorage-ByteHrs"),
    # Change data capture and streams
    "kinesis_cdc": PriceSpec(
        "ChangeDataCaptureUnits-Kinesis"
    ),  # USD per change data capture unit
    "streams_read": PriceSpec("Streams-Requests"),  # USD per streams read request unit
}

DEFAULT_REQUIRED: Tuple[str, ...] = (
    "on_demand_read",
    "on_demand_write",
    "on_demand_read_ia",
    "on_demand_write_ia",
    "provisioned_read",
    "provisioned_write",
    "provisioned_read_ia",
    "provisioned_write_ia",
    "storage",
    "storage_ia",
)

# Usage types that carry a region prefix in every region (including us-east-1),
# used to discover the prefix for the region.
_ALWAYS_PREFIXED = re.compile(
    r"^(?P<prefix>[A-Z0-9]+)-"
    r"(TimedPITRStorage-ByteHrs|ExportDataSize-Bytes|ImportDataSize-Bytes)$"
)


class MissingPricingError(LookupError):
    """Raised when required prices are not in pricing_data for a region."""

    def __init__(self, region_code: str, missing: List[str]):
        self.region_code = region_code
        self.missing = missing
        super().__init__(
            f"Missing DynamoDB pricing for {region_code}: {', '.join(missing)}. "
            "Run pricing collection for this region first."
        )


@dataclass(frozen=True)
class RegionPricing:
    """Named prices for one region, plus when they were collected."""

    region_code: str
    collected_at: datetime
    prices: Mapping[str, float]

    def __getitem__(self, key: str) -> float:
        return self.prices[key]


def get_region_pricing(
    connection,
    region_code: str,
    required: Iterable[str] = DEFAULT_REQUIRED,
) -> RegionPricing:
    """Return every named price available for ``region_code``.

    Args:
        connection: DuckDB connection with the pricing_data table
        region_code: AWS region code, e.g. "us-west-2"
        required: Price keys that must be present (default: core throughput and storage)

    Raises:
        ValueError: if ``required`` names an unknown price key
        MissingPricingError: if the region has no pricing data or lacks a required price
    """
    required = list(required)
    unknown = [k for k in required if k not in PRICE_KEYS]
    if unknown:
        raise ValueError(f"Unknown price keys: {', '.join(unknown)}")

    rows = connection.execute(
        """
        SELECT usage_type, COALESCE(term_type, 'OnDemand'), lease_contract_length,
               unit, end_range, price_per_unit, collected_at
        FROM pricing_data
        WHERE region_code = ?
        """,
        (region_code,),
    ).fetchall()
    if not rows:
        raise MissingPricingError(region_code, required or ["(no pricing data)"])

    prefix = _region_prefix(row[0] for row in rows)
    prices: Dict[str, float] = {}
    for key, spec in PRICE_KEYS.items():
        usage_types = (
            {spec.usage_type, f"{prefix}-{spec.usage_type}"}
            if prefix
            else {spec.usage_type}
        )
        matches = {
            float(price)
            for usage_type, term_type, lease, unit, end_range, price, _ in rows
            if usage_type in usage_types
            and _is_paid_dimension(spec, term_type, lease, unit, end_range, price)
        }
        if len(matches) > 1:
            raise ValueError(
                f"Ambiguous {key} price for {region_code}: {sorted(matches)}"
            )
        if matches:
            prices[key] = matches.pop()

    missing = [k for k in required if k not in prices]
    if missing:
        raise MissingPricingError(region_code, missing)

    return RegionPricing(
        region_code=region_code,
        collected_at=max(row[6] for row in rows),
        prices=prices,
    )


def _region_prefix(usage_types: Iterable[str]) -> Optional[str]:
    for usage_type in usage_types:
        match = _ALWAYS_PREFIXED.match(usage_type or "")
        if match:
            return match.group("prefix")
    return None


def _is_paid_dimension(
    spec: PriceSpec, term_type, lease, unit, end_range, price
) -> bool:
    if term_type != spec.term_type or price is None or float(price) <= 0:
        return False
    if spec.term_type == RESERVED:
        if lease != spec.lease:
            return False
        return (
            unit == UPFRONT_UNIT
            if spec.upfront
            else unit != UPFRONT_UNIT and end_range == "Inf"
        )
    return end_range == "Inf"
