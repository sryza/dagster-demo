import numpy as np
import pandas as pd
from scipy import optimize

from dagster import AssetIn, asset


def model_func(x, a, b):
    return a * np.exp(b * (x / 10**18 - 1.6095))


@asset(
    ins={"daily_order_summary": AssetIn(key_prefix=["duckdb", "dbt_schema"])},
    compute_kind="ml_tool",
    key_prefix=["duckdb", "forecasting"],
)
def predicted_orders(daily_order_summary: pd.DataFrame) -> pd.DataFrame:
    """Predicted orders for the next 30 days based on the fit paramters"""
    xdata = daily_order_summary.date.astype(np.int64)
    ydata = daily_order_summary.n_orders
    a, b = tuple(optimize.curve_fit(f=model_func, xdata=xdata, ydata=ydata, p0=[10, 100])[0])

    start_date = daily_order_summary.date.max()
    future_dates = pd.date_range(
        start=start_date, end=pd.to_datetime(start_date) + pd.DateOffset(days=30)
    )
    predicted_data = model_func(x=future_dates.astype(np.int64), a=a, b=b)
    return pd.DataFrame({"order_date": future_dates, "num_orders": predicted_data})
