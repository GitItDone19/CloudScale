"""Pure DataFrame transformations; no Python UDFs or driver-side row collection."""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark.utils.schemas import FIELDS


def normalize(df, source):
    for name in FIELDS[source]:
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast("string"))
        df = df.withColumn(
            name, F.when(F.length(F.trim(F.col(name))) > 0, F.trim(F.col(name)))
        )
    if "_corrupt_record" not in df.columns:
        df = df.withColumn("_corrupt_record", F.lit(None).cast("string"))
    if "_raw_record" not in df.columns:
        df = df.withColumn(
            "_raw_record", F.to_json(F.struct(*[F.col(c) for c in FIELDS[source]]))
        )
    return df


def mark_errors(df, rules):
    rules = [(F.col("_corrupt_record").isNotNull(), "ERR_MALFORMED_RECORD")] + rules
    errors = F.array(*[F.when(condition, F.lit(code)) for condition, code in rules])
    return df.withColumn(
        "_error_codes", F.filter(errors, lambda value: value.isNotNull())
    ).withColumn("_error_code", F.element_at("_error_codes", 1))


def missing(*names):
    condition = F.lit(False)
    for name in names:
        condition = condition | F.col(name).isNull()
    return condition


def timestamps(df, *names):
    for name in names:
        df = df.withColumn(name, F.try_to_timestamp(F.col(name)))
    return df


def invalid_number(name, lower, upper=None):
    c = F.col(name)
    condition = c.isNull() | F.isnan(c) | (F.abs(c) == float("inf")) | (c <= lower)
    if upper is not None:
        condition = condition | (c >= upper)
    return condition


def transform_orders(df, as_of):
    df = normalize(df, "postgres_orders")
    df = timestamps(df, "order_timestamp").withColumn(
        "declared_value", F.expr("try_cast(declared_value as decimal(12,2))")
    )
    df = df.withColumn("customer_country", F.upper("customer_country")).withColumn(
        "currency", F.upper("currency")
    )
    return mark_errors(
        df,
        [
            (missing("order_id", "merchant_id"), "ERR_NULL_KEY"),
            (
                F.col("declared_value").isNull() | (F.col("declared_value") <= 0),
                "ERR_INVALID_VALUE",
            ),
            (missing("order_timestamp"), "ERR_INVALID_TIMESTAMP"),
            (
                F.col("order_timestamp") > F.lit(as_of).cast("timestamp"),
                "ERR_FUTURE_TIMESTAMP",
            ),
            (
                F.col("customer_postal_code").isNull()
                | ~F.col("customer_postal_code").rlike(
                    "^[A-Za-z0-9][A-Za-z0-9 -]{0,19}$"
                ),
                "ERR_INVALID_POSTAL_CODE",
            ),
            (
                F.col("customer_country").isNull()
                | ~F.col("customer_country").rlike("^[A-Z]{2}$"),
                "ERR_INVALID_COUNTRY",
            ),
            (
                F.col("currency").isNull()
                | ~F.col("currency").isin("EUR", "USD", "GBP"),
                "ERR_INVALID_CURRENCY",
            ),
        ],
    )


def transform_merchants(df, as_of):
    df = normalize(df, "postgres_merchants")
    df = (
        timestamps(df, "created_at")
        .withColumn("country_code", F.upper("country_code"))
        .withColumn("merchant_tier", F.upper("merchant_tier"))
    )
    return mark_errors(
        df,
        [
            (missing("merchant_id"), "ERR_NULL_KEY"),
            (missing("merchant_name"), "ERR_MISSING_FIELD"),
            (missing("created_at"), "ERR_INVALID_TIMESTAMP"),
            (
                F.col("created_at") > F.lit(as_of).cast("timestamp"),
                "ERR_FUTURE_TIMESTAMP",
            ),
            (
                F.col("merchant_tier").isNull()
                | ~F.col("merchant_tier").isin("PLATINUM", "GOLD", "SILVER", "BRONZE"),
                "ERR_INVALID_TIER",
            ),
            (
                F.col("country_code").isNull()
                | ~F.col("country_code").rlike("^[A-Z]{2}$"),
                "ERR_INVALID_COUNTRY",
            ),
        ],
    )


def transform_wms(df, as_of):
    df = normalize(df, "wms_picks")
    df = timestamps(df, "picked_at", "packed_at", "dispatched_at").withColumn(
        "parcel_weight_kg", F.expr("try_cast(parcel_weight_kg as double)")
    )
    return mark_errors(
        df,
        [
            (missing("wms_record_id", "order_id", "warehouse_id"), "ERR_NULL_KEY"),
            (invalid_number("parcel_weight_kg", 0.01, 1000), "ERR_INVALID_WEIGHT"),
            (
                missing("picked_at", "packed_at", "dispatched_at"),
                "ERR_INVALID_TIMESTAMP",
            ),
            (
                (F.col("packed_at") < F.col("picked_at"))
                | (F.col("dispatched_at") < F.col("packed_at")),
                "ERR_TIMESTAMP_SEQUENCE",
            ),
        ],
    )


def transform_carrier(df, as_of):
    df = normalize(df, "carrier_events")
    df = df.withColumn(
        "tracking_number", F.coalesce("tracking_number", "tracking_code")
    )
    df = timestamps(df, "scan_timestamp", "received_at")
    fee_present = F.col("customs_fee").isNotNull()
    df = df.withColumn("_fee_present", fee_present).withColumn(
        "customs_fee", F.expr("try_cast(customs_fee as decimal(12,2))")
    )
    df = df.withColumn("currency", F.upper("currency")).withColumn(
        "event_type", F.upper("event_type")
    )
    return mark_errors(
        df,
        [
            (missing("event_id", "tracking_number", "carrier_id"), "ERR_NULL_KEY"),
            (missing("scan_timestamp", "received_at"), "ERR_INVALID_TIMESTAMP"),
            (F.col("received_at") < F.col("scan_timestamp"), "ERR_TIMESTAMP_SEQUENCE"),
            (
                F.col("event_type").isNull()
                | ~F.col("event_type").isin(
                    "ACCEPTED",
                    "IN_TRANSIT",
                    "OUT_FOR_DELIVERY",
                    "DELIVERED",
                    "FAILED_ATTEMPT",
                    "EXCEPTION",
                ),
                "ERR_INVALID_EVENT_TYPE",
            ),
            (
                F.col("_fee_present")
                & (F.col("customs_fee").isNull() | (F.col("customs_fee") < 0)),
                "ERR_INVALID_VALUE",
            ),
            (
                (F.col("_fee_present") | F.col("currency").isNotNull())
                & (
                    F.col("currency").isNull()
                    | ~F.col("currency").isin("EUR", "USD", "GBP")
                ),
                "ERR_INVALID_CURRENCY",
            ),
        ],
    ).drop("_fee_present", "tracking_code")


def split_quality(df):
    clean = df.filter(F.col("_error_code").isNull()).drop(
        "_error_code", "_error_codes", "_corrupt_record", "_raw_record"
    )
    rejected = df.filter(F.col("_error_code").isNotNull())
    return clean, rejected


def deduplicate_events(df):
    # Reject invalid rows first so a bad newer row cannot hide a valid scan.
    # A payload hash makes ties deterministic across repartitioning and retries.
    tie = F.sha2(
        F.to_json(
            F.struct(*[F.col(c) for c in sorted(df.columns) if not c.startswith("_")])
        ),
        256,
    )
    ordering = [F.col("scan_timestamp").desc(), F.col("received_at").desc(), tie.desc()]
    if "_source_file" in df.columns:
        ordering.append(F.col("_source_file").asc_nulls_last())
    window = Window.partitionBy("event_id").orderBy(*ordering)
    return (
        df.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )
