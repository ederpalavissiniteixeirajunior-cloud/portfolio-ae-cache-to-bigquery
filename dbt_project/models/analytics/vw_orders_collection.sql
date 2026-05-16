
with orders as (
    select * from {{ ref('fct_orders') }}
),

collections as (
    select * from {{ ref('dim_collection') }}
),

intermediate as (
    select
        c.collection_name,
        ROUND(SUM(o.vl_original_total),2) AS vl_original_total,
    from collections c
    left join orders o
    ON c.sk_collection_version = o.sk_collection_version
    group by 1

)

select * from intermediate
where vl_original_total IS NOT NULL