select
        date,
        count(*) as n_orders,
        count(distinct u.company) as n_companies,
        sum(order_total) as total_revenue
from {{ ref("stg_orders") }} o
left join {{ ref("stg_users") }} u on (o.user_id = u.user_id)
group by 1
