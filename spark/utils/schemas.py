"""Explicit raw schemas: parse as strings, then validate safe typed conversions."""

from pyspark.sql.types import StringType, StructField, StructType

FIELDS = {
    "postgres_orders": [
        "order_id",
        "merchant_id",
        "customer_postal_code",
        "customer_country",
        "declared_value",
        "currency",
        "order_timestamp",
    ],
    "postgres_merchants": [
        "merchant_id",
        "merchant_name",
        "merchant_tier",
        "country_code",
        "created_at",
    ],
    "wms_picks": [
        "wms_record_id",
        "order_id",
        "warehouse_id",
        "parcel_weight_kg",
        "box_type",
        "picked_at",
        "packed_at",
        "dispatched_at",
    ],
    "carrier_events": [
        "event_id",
        "tracking_number",
        "order_id",
        "carrier_id",
        "event_type",
        "scan_timestamp",
        "received_at",
        "location_code",
        "exception_reason",
        "tracking_code",
        "customs_fee",
        "currency",
    ],
}


def raw_schema(source):
    return StructType(
        [
            StructField(name, StringType(), True)
            for name in FIELDS[source] + ["_corrupt_record"]
        ]
    )
