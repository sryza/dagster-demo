from dagster import (
    RunRequest,
    ScheduleDefinition,
    define_asset_job,
    AssetKey,
    AssetSelection,
    sensor,
)

from assets_dbt_python.utils import new_orders_are_available, new_users_are_available


@sensor(
    job=define_asset_job(
        "orders_and_users_job", selection=AssetSelection.keys("stg_orders", "stg_users").upstream()
    ),
)
def orders_and_users_sensor(context):
    assets = []
    if new_orders_are_available():
        assets.extend([AssetKey(["raw_data", "orders"]), AssetKey("stg_orders")])
    if new_users_are_available():
        assets.extend([AssetKey(["raw_data", "users"]), AssetKey("stg_users")])

    if assets:
        return RunRequest(asset_selection=assets)


predicted_orders_schedule = ScheduleDefinition(
    cron_schedule="@daily",
    job=define_asset_job(
        "predicted_orders_job",
        selection=AssetSelection.keys("daily_order_summary", "predicted_orders"),
    ),
)
