from assets_dbt_python import repository as repository_module
from assets_dbt_python.repository import dbt_cli_resource
from assets_dbt_python.utils import create_temporary_snowflake_database
from dagster import load_assets_from_modules, materialize, with_resources
from dagster_snowflake import build_snowflake_io_manager
from dagster_snowflake_pandas import SnowflakePandasTypeHandler


SNOWFLAKE_CONFIG = {
    "account": {"env": "SNOWFLAKE_ACCOUNT"},
    "user": {"env": "SNOWFLAKE_USER"},
    "password": {"env": "SNOWFLAKE_PASSWORD"},
    "warehouse": "ELEMENTL",
}


def test_all_assets():
    assets = load_assets_from_modules([repository_module])

    with create_temporary_snowflake_database() as snowflake_database_name:
        assets_with_resources = with_resources(
            assets,
            resource_defs={
                "io_manager": build_snowflake_io_manager([SnowflakePandasTypeHandler()]).configured(
                    {"database": snowflake_database_name, **SNOWFLAKE_CONFIG}
                ),
                "dbt": dbt_cli_resource,
            },
        )

        result = materialize(assets_with_resources)

        predicted_orders = result.output_for_node("predicted_orders")
        assert all(num_orders > 0 for num_orders in predicted_orders.num_orders)
        assert predicted_orders.shape[0] == 31
