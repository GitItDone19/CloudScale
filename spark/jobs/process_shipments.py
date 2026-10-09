"""Validate Bronze orders, merchant snapshots, and warehouse picks."""

from spark.utils.pipeline import cli, run_sources
from spark.utils.quality import transform_merchants, transform_orders, transform_wms

TRANSFORMS = {
    "postgres_orders": transform_orders,
    "postgres_merchants": transform_merchants,
    "wms_picks": transform_wms,
}


def run(spark, **kwargs):
    return run_sources(spark, TRANSFORMS, **kwargs)


if __name__ == "__main__":
    cli(TRANSFORMS, "CloudScale shipment cleansing")
