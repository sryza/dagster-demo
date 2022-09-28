from dagster import Output, asset, repository, with_resources
from dagster._utils import file_relative_path
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project
from dagster_snowflake import build_snowflake_io_manager
from dagster_snowflake_pandas import SnowflakePandasTypeHandler
from pandas import DataFrame, DateOffset, date_range, to_datetime
from scipy import optimize

from assets_dbt_python.utils import (connect_to_app_db, fetch_orders,
                                     fetch_users, model_func, to_epoch_seconds)

DBT_PROJECT_DIR = file_relative_path(__file__, "../dbt_project")
DBT_PROFILES_DIR = file_relative_path(__file__, "../dbt_project/config")


@asset(compute_kind="ingest", key_prefix="raw_data")
def users() -> DataFrame:
    return fetch_users(conn=connect_to_app_db())


@asset(compute_kind="ingest", key_prefix="raw_data")
def orders() -> DataFrame:
    return fetch_orders(conn=connect_to_app_db())


dbt_assets = load_assets_from_dbt_project(
    DBT_PROJECT_DIR, DBT_PROFILES_DIR, node_info_to_group_fn=lambda _: "default"
)


@asset(compute_kind="ml")
def predicted_orders(daily_order_summary: DataFrame) -> Output[DataFrame]:
    xdata = to_epoch_seconds(daily_order_summary.order_date)
    ydata = daily_order_summary.n_orders
    a, b = tuple(optimize.curve_fit(f=model_func, xdata=xdata, ydata=ydata, p0=[10, 100])[0])

    start_date = daily_order_summary.order_date.max()
    future_dates = date_range(start_date, to_datetime(start_date) + DateOffset(days=30))
    predicted_data = model_func(x=to_epoch_seconds(future_dates), a=a, b=b)
    return Output(
        DataFrame({"order_date": future_dates, "num_orders": predicted_data}),
        metadata={"mean_squared_error": (ydata - model_func(xdata, a, b)).mean(), "a": a, "b": b},
    )


@repository
def assets_dbt_python():
    return with_resources(
        dbt_assets + [users, orders] + [predicted_orders],
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
            "dbt": dbt_cli_resource.configured(
                {"project_dir": DBT_PROJECT_DIR, "profiles_dir": DBT_PROFILES_DIR}
            ),
        },
    )
