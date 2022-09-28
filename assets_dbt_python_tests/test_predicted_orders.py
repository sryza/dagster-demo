from pandas import DataFrame

from assets_dbt_python.repository import predicted_orders


def test_predicted_orders():
    daily_order_summary = DataFrame(
        {"order_date": ["2022-01-01", "2022-01-02", "2022-01-03"], "n_orders": [2, 4, 5]}
    )

    result = predicted_orders(daily_order_summary).value

    assert all(num_orders > 0 for num_orders in result.num_orders)
    assert result.shape[0] == 31
