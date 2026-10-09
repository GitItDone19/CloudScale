"""Offline tests using real BigQuery SDK objects and fake authenticated clients."""

from datetime import date
from unittest.mock import Mock

import pytest

pytest.importorskip("google.cloud.bigquery")
from google.cloud import bigquery

from warehouse.contracts import WarehouseConfig, plan, STRING_FIELDS
from warehouse.bigquery_setup import (
    apply_setup,
    table_definition,
    verify,
    external_signature,
)


@pytest.fixture
def config():
    return WarehouseConfig("cloudscale-test", "cloudscale-silver-test")


def test_plan_and_sdk_external_tables_require_date_filters(config):
    manifest = plan(config)
    assert len(manifest["datasets"]) == 4
    for source in STRING_FIELDS:
        table = table_definition(config, source)
        api = table.to_api_repr()["externalDataConfiguration"]
        assert api["sourceFormat"] == "PARQUET"
        assert api["hivePartitioningOptions"] == {
            "mode": "CUSTOM",
            "sourceUriPrefix": config.source_prefix(source) + "/{dt:DATE}",
            "requirePartitionFilter": True,
        }
        assert api["sourceUris"] == [config.source_prefix(source) + "/*.parquet"]
        assert "dt" not in {f.name for f in table.external_data_configuration.schema}
        assert external_signature(
            bigquery.Table.from_api_repr(table.to_api_repr())
        ) == external_signature(table)
    fields = {
        f.name: f
        for f in table_definition(
            config, "postgres_orders"
        ).external_data_configuration.schema
    }
    assert fields["declared_value"].precision == 12
    assert fields["declared_value"].scale == 2


@pytest.mark.parametrize(
    "kwargs",
    [
        {"project": "invalid`sql"},
        {"bucket": "gs://bucket/path"},
        {"prefix": "../outside"},
        {"maximum_bytes_billed": 0},
        {"maximum_bytes_billed": 104857601},
    ],
)
def test_invalid_configuration_rejected(kwargs):
    values = dict(project="cloudscale-test", bucket="cloudscale-silver-test")
    values.update(kwargs)
    with pytest.raises(ValueError):
        WarehouseConfig(**values)


def test_bootstrap_idempotent_and_preserves_existing_tables(config):
    client = Mock()
    client.create_dataset.side_effect = lambda dataset, **kwargs: dataset
    client.create_table.side_effect = lambda table, **kwargs: table
    apply_setup(client, config)
    apply_setup(client, config)
    assert client.create_dataset.call_count == client.create_table.call_count == 8
    assert all(call.kwargs["exists_ok"] for call in client.create_table.call_args_list)
    client.update_table.assert_not_called()
    client.delete_table.assert_not_called()


def test_location_conflict_stops_before_table_creation(config):
    client = Mock()
    wrong = bigquery.Dataset("cloudscale-test.raw_staging")
    wrong.location = "EU"
    client.create_dataset.return_value = wrong
    with pytest.raises(ValueError, match="location"):
        apply_setup(client, config)
    client.create_table.assert_not_called()


def test_existing_native_table_is_not_replaced(config):
    client = Mock()
    client.create_dataset.side_effect = lambda dataset, **kwargs: dataset
    client.create_table.return_value = bigquery.Table(
        "cloudscale-test.raw_staging.postgres_orders"
    )
    with pytest.raises(ValueError, match="differs"):
        apply_setup(client, config)
    client.delete_table.assert_not_called()


@pytest.mark.parametrize("execute", [False, True])
def test_verification_uses_parameters_and_scan_limit(config, execute):
    job = Mock(total_bytes_processed=100, total_bytes_billed=100)
    job.result.return_value = [{"records": 10, "wrong_batch_date": 0}]
    client = Mock()
    client.query.return_value = job
    result = verify(client, config, "2026-10-01", execute=execute)
    assert len(result) == 4
    for call in client.query.call_args_list:
        assert "WHERE dt = @batch_date" in call.args[0]
        cfg = call.kwargs["job_config"]
        assert cfg.maximum_bytes_billed == 104857600
        assert cfg.dry_run == (not execute)
        assert cfg.query_parameters[0].value == date(2026, 10, 1)
        assert call.kwargs["location"] == "US"
    if not execute:
        job.result.assert_not_called()


def test_wrong_batch_metadata_fails_live_verification(config):
    client = Mock()
    client.query.return_value.result.return_value = [
        {"records": 1, "wrong_batch_date": 1}
    ]
    with pytest.raises(ValueError, match="metadata"):
        verify(client, config, "2026-10-01", execute=True)
