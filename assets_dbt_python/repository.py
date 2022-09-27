import os

import numpy as np
from assets_dbt_python.resources import duckdb_io_manager
from assets_dbt_python.utils import connect_to_app_db, fetch_orders, fetch_users, model_func
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project
from pandas import DataFrame, DateOffset, date_range, to_datetime
from scipy import optimize

from dagster import Output, asset, repository, with_resources
from dagster._utils import file_relative_path

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
    xdata = daily_order_summary.date.astype(np.int64)
    ydata = daily_order_summary.n_orders
    a, b = tuple(optimize.curve_fit(f=model_func, xdata=xdata, ydata=ydata, p0=[10, 100])[0])

    start_date = daily_order_summary.date.max()
    future_dates = date_range(start_date, to_datetime(start_date) + DateOffset(days=30))
    predicted_data = model_func(x=future_dates.astype(np.int64), a=a, b=b)
    return Output(
        DataFrame({"order_date": future_dates, "num_orders": predicted_data}),
        metadata={"mean_squared_error": (ydata - model_func(xdata, a, b)).mean(), "a": a, "b": b},
    )


@repository
def assets_dbt_python():
    return with_resources(
        dbt_assets + [users, orders] + [predicted_orders],
        resource_defs={
            "io_manager": duckdb_io_manager.configured(
                {"duckdb_path": os.path.join(DBT_PROJECT_DIR, "example.duckdb")}
            ),
            "dbt": dbt_cli_resource.configured(
                {"project_dir": DBT_PROJECT_DIR, "profiles_dir": DBT_PROFILES_DIR}
            ),
        },
    )
