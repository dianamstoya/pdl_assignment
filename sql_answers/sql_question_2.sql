with topC as
-- since the quantities are not given, and each order_id + order_item 
-- has only 1 product_id associated, we *ASSUME* that each order_id || order_item_id = quantity of 1 of the product
(
    select
        p.product_category_name, 
        count(distinct (oi.order_id || oi.order_item_id)) as Qty_sub
    from products p 
        join order_items oi on p.product_id = oi.product_id
        join orders o on o.order_id = oi.order_id
    where year(o.order_purchase_timestamp) = 2017
        and month(o.order_purchase_timestamp) = 11
    group by
        p.product_category_name
    order by Qty_sub desc
    limit 3
),
wkly as 
(
    select 
        weekofyear(o.order_purchase_timestamp) as Wk_nr,
        p.product_category_name,
		-- each row in the order_items table is a unique order_id || order_item_id --> qty: 1
		-- the total value of the order item is then 1*price, therefore:
        sum(oi.price) as GMV
    from orders o 
        join order_items oi on o.order_id = oi.order_id
        join products p on p.product_id = oi.product_id
        join topC c on c.product_category_name = p.product_category_name -- inner join limits to only the top 3 cat.
    where year(o.order_purchase_timestamp) = 2017
    group by
        weekofyear(o.order_purchase_timestamp),
        p.product_category_name
    order by product_category_name, Wk_nr
)

select
    product_category_name, 
    Wk_nr, 
    GMV,
    -- using window functions to calculate the running totals
    -- and to shift the current value by one row back (up)
    sum(GMV) 
        over (
            partition by product_category_name 
            order by Wk_nr
        ) as GMV_run_total,
    -- the delta % is (current GMV - prev GMV) / prev GMV
    (GMV - lag(GMV) 
            over (partition by product_category_name
                  order by Wk_nr
            ) ) / lag(GMV) 
                over (partition by product_category_name
                      order by Wk_nr
                ) * 100 as growth_rate_pct
from wkly;
