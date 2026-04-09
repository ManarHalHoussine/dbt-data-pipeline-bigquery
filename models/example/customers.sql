
with customers as (

     select
       customer_id,
       first_name,
       last_name
     from {{ref("stg_customers")}}
),

orders as (
   select
       order_id,
       customer_id,
       order_date,
       status
   from {{ref("stg_orders")}}
),

customers_orders as (
    select
         customer_id,
         min(order_date) as first_order,
         max(order_date) as recent_order,
         count(order_id) as number_orders
    from orders
    group by 1
),


final as (
    select
      customers.customer_id,
      customers.first_name,
      customers.last_name,
      customers_orders.first_order as first_order,
      customers_orders.recent_order as recent_order,
      coalesce(customers_orders.number_orders,0) as number_orders
    from customers
    left join customers_orders
       on customers.customer_id = customers_orders.customer_id


)

select * from final