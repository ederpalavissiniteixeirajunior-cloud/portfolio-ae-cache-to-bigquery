
with orders as (
    select * from {{ ref('fct_orders') }}
),

dim_time as (
    select * from {{ ref('dim_calendar') }}
),

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

intermediate as (
    select
       o.cd_order,
       o.vl_original_total,
       t.dt_date,
       t.ds_month,
       c.ds_corporate_name
    FROM
        orders o
    left join
        dim_time t
    ON
        o.sk_time_version = t.sk_time_version
    left join
        dim_customers c
    ON
        o.sk_customer_version = c.sk_customer_version
    where
        c.is_current = true


)

select * from intermediate