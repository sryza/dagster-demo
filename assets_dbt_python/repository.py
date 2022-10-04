from dagster import load_assets_from_package_module, repository, with_resources
from dagster._utils import file_relative_path
from dagster_dbt import dbt_cli_resource
from dagster_snowflake import build_snowflake_io_manager
from dagster_snowflake_pandas import SnowflakePandasTypeHandler

from . import assets as assets_package

DBT_PROJECT_DIR = file_relative_path(__file__, "../dbt_project")
DBT_PROFILES_DIR = file_relative_path(__file__, "../dbt_project/config")

dbt_resource = dbt_cli_resource.configured(
    {"project_dir": DBT_PROJECT_DIR, "profiles_dir": DBT_PROFILES_DIR}
)


@repository
def assets_dbt_python():
    return with_resources(
        load_assets_from_package_module(assets_package),
        resource_defs={
            "io_manager": build_snowflake_io_manager([SnowflakePandasTypeHandler()]).configured(
                {
                    "account": {"env": "SNOWFLAKE_ACCOUNT"},
                    "user": {"env": "SNOWFLAKE_USER"},
                    "password": {"env": "SNOWFLAKE_PASSWORD"},
                    "database": "DEV_SANDY",
                    "warehouse": "ELEMENTL",
                }
            ),
            "dbt": dbt_resource,
        },
    )
