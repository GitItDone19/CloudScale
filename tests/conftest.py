import pytest


@pytest.fixture(scope="session")
def spark_session():
    pytest.importorskip(
        "pyspark",
        reason="Install requirements-spark.txt or use the spark-jobs container",
    )
    from spark.utils.spark_builder import build_spark

    session = build_spark("CloudScale regression tests", "local[2]")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
