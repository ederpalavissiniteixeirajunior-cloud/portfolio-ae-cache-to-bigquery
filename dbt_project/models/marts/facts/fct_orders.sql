{{ config(
    materialized='table',
    partition_by={
      "field": "dt_issued",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["sk_customer_version", "sk_sales_representative_version"]
) }}

with orders as (
    select * from {{ ref('itm_f_orders') }}
),

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

dim_representatives as (
    select * from {{ ref('dim_sales_representative') }}
),

dim_collections as (
    select * from {{ ref('dim_collection') }}
),

dim_calendar as (
    select * from {{ ref('dim_calendar') }}
),

final_join as (
    select
        f.sk_order_version,
        c.sk_customer_version,
        r.sk_sales_representative_version,
        col.sk_collection_version,
        cal.sk_time_version,
        f.cd_order,
        f.vl_original_total,
        f.nm_status,
        f.dt_issued,
        current_timestamp() as updated_at

    from orders f
    
    left join dim_customers c
        on f.cd_customer = c.cd_customer
        and f.dt_issued between c.valid_from and coalesce(c.valid_to, '9999-12-31')

    left join dim_representatives r
        on f.cd_sales_representative = r.cd_sales_representative
        and f.dt_issued between r.valid_from and coalesce(r.valid_to, '9999-12-31')
    
    left join dim_collections col 
        on f.id_collection = col.id_collection

    left join dim_calendar cal 
        on f.dt_issued = cal.dt_date
)

select * from final_join