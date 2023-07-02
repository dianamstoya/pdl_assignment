with cte1 as -- lowest level aggregation
(
    select 
        month(o.order_purchase_timestamp) as Mnth,
        weekofyear(o.order_purchase_timestamp) as Wk_nr,
        date(o.order_purchase_timestamp) as Day_dt,
        oi.seller_id,
        count(distinct o.order_id) as No_orders
    from orders o 
        join order_items oi on o.order_id = oi.order_id
    where year(order_purchase_timestamp) = 2017
    group by 
        month(o.order_purchase_timestamp),
        weekofyear(o.order_purchase_timestamp),
        date(o.order_purchase_timestamp),
        oi.seller_id
    order by Mnth, Wk_nr, No_orders desc
),
cte_m as -- monthly aggregation
(
    select 
        Mnth, 
        seller_id
    from cte1
    group by Mnth, seller_id
    having sum(No_orders) >= 25
),
cte_w as -- for weekly aggregation, first filter out sellers
-- which do not fulfill the criteria
(
    select
        Mnth, 
        Wk_nr,
        seller_id
    from cte1
    group by Mnth,  Wk_nr, seller_id
    having sum(No_orders) >= 5
), 
cte_w2 as -- then aggregate to get the count of such sellers per week
(
    select 
        Mnth, 
        Wk_nr,
        count(distinct seller_id) as wkly_sellers
    from cte_w
    group by Mnth, Wk_nr
), 
cte_d as -- for daily aggregation, first filter out sellers
-- which do not fulfill the criteria
(
    select 
        Mnth,
        Day_dt, 
        seller_id
    from cte1
    group by Mnth, Day_dt, seller_id
    having sum(No_orders) >= 1
), 
cte_d2 as -- then aggregate to get the count of such sellers per day
(
    select 
        Mnth, 
        Day_dt, 
        count(distinct seller_id) as dly_sellers
    from cte_d
    group by Mnth, Day_dt
)

select -- in the final aggregation create the averages
    m.Mnth, 
    count(distinct m.seller_id) as monthly_act_sellers,
    avg(wkly_sellers) as avg_weekly_act_sellers,
    avg(dly_sellers) as avg_daily_act_sellers
from cte_m as m 
    left join cte_w2 as w on m.Mnth = w.Mnth
    left join cte_d2 as d on m.Mnth = d.Mnth
group by m.Mnth
order by 1;
