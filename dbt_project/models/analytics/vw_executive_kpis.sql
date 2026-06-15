with sales_agg as (
    select
        col.collection_name,
        sum(f.vl_original_total) as total_sales,
        count(distinct f.cd_customer) as total_customers
    from {{ ref('fct_orders') }} f
    join {{ ref('dim_collection') }} col 
        on f.sk_collection_version = col.sk_collection_version
    where
        f.nm_status <> 'CANCELADO'
    group by 1
),

target_agg as (
    select
        col.collection_name,
        sum(t.sales_target) as total_target
    from `bronze.stg_sales_target` t
    join (
        select distinct id_collection, collection_name from {{ ref('dim_collection') }}
    ) col on t.id_collection = col.id_collection
    group by 1
)

select
    coalesce(s.collection_name, t.collection_name) as collection_name,
    coalesce(s.total_sales, 0) as total_sales,
    coalesce(s.total_customers, 0) as total_customers,
    coalesce(t.total_target, 0) as total_target,
    safe_divide(coalesce(s.total_sales, 0), coalesce(t.total_target, 0)) as target_attainment
from sales_agg s
full outer join target_agg t on s.collection_name = t.collection_name
