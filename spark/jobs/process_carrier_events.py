"""Validate and deduplicate Bronze carrier events, preserving late arrivals."""

from spark.utils.pipeline import cli, run_sources
from spark.utils.quality import transform_carrier

TRANSFORMS = {"carrier_events": transform_carrier}


def run(spark, **kwargs):
    return run_sources(spark, TRANSFORMS, **kwargs)


if __name__ == "__main__":
    cli(TRANSFORMS, "CloudScale carrier event cleansing")
