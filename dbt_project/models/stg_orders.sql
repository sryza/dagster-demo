select
        user_id,
        quantity,
        purchase_price,
        sku,
        dt,
        to_date(dt) as order_date,
        quantity * purchase_price as order_total
from {{ source('raw_data', 'orders') }}
