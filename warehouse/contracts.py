"""Warehouse contracts matching the local Silver Parquet outputs."""

from dataclasses import dataclass
import re

DATASETS = ("raw_staging", "analytics_core", "analytics_marts", "ops_monitoring")
STRING_FIELDS = {
    "postgres_orders": "order_id merchant_id customer_postal_code customer_country currency",
    "postgres_merchants": "merchant_id merchant_name merchant_tier country_code",
    "wms_picks": "wms_record_id order_id warehouse_id box_type",
    "carrier_events": "event_id tracking_number order_id carrier_id event_type location_code exception_reason currency",
}
TYPED_FIELDS = {
    "postgres_orders": {"declared_value": "NUMERIC", "order_timestamp": "TIMESTAMP"},
    "postgres_merchants": {"created_at": "TIMESTAMP"},
    "wms_picks": {
        "parcel_weight_kg": "FLOAT",
        "picked_at": "TIMESTAMP",
        "packed_at": "TIMESTAMP",
        "dispatched_at": "TIMESTAMP",
    },
    "carrier_events": {
        "scan_timestamp": "TIMESTAMP",
        "received_at": "TIMESTAMP",
        "customs_fee": "NUMERIC",
    },
}


def fields(source):
    return {
        **{name: "STRING" for name in STRING_FIELDS[source].split()},
        **TYPED_FIELDS[source],
        "_source_file": "STRING",
        "_batch_date": "DATE",
        "_processed_at": "TIMESTAMP",
    }


@dataclass(frozen=True)
class WarehouseConfig:
    project: str
    bucket: str
    location: str = "US"
    prefix: str = "silver/v1"
    maximum_bytes_billed: int = 104857600

    def __post_init__(self):
        if not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]", self.project):
            raise ValueError("Use a valid Google Cloud project ID (6–30 characters)")
        if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]", self.bucket):
            raise ValueError("Use a bucket name, without gs:// or a path")
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9-]*", self.location):
            raise ValueError("Invalid BigQuery location")
        if not re.fullmatch(r"[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*", self.prefix):
            raise ValueError("Prefix must contain safe path segments")
        if not 0 < self.maximum_bytes_billed <= 104857600:
            raise ValueError("Query limit must be between 1 byte and 100 MiB")

    def source_prefix(self, source):
        if source not in STRING_FIELDS:
            raise ValueError("Unknown Silver source")
        return f"gs://{self.bucket}/{self.prefix}/{source}"

    def table_id(self, source):
        if source not in STRING_FIELDS:
            raise ValueError("Unknown Silver source")
        return f"{self.project}.raw_staging.{source}"


def plan(config):
    return {
        "project": config.project,
        "location": config.location,
        "datasets": [f"{config.project}.{name}" for name in DATASETS],
        "tables": [
            {
                "table": config.table_id(source),
                "format": "PARQUET",
                "source_uris": [config.source_prefix(source) + "/*.parquet"],
                "hive_prefix": config.source_prefix(source) + "/{dt:DATE}",
                "require_partition_filter": True,
                "schema": fields(source),
            }
            for source in STRING_FIELDS
        ],
        "maximum_bytes_billed": config.maximum_bytes_billed,
    }
