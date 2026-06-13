{{ config(
    materialized='table',
    partition_by={
      "field": "dt_issued",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["sk_customer_version", "sk_product_version", "sk_sales_representative_version"]
) }}

with item_sales as (
    select * from {{ ref('itm_f_order_items') }}
),

order_header as (
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

dim_products as (
    select * from {{ ref('dim_products') }}
),

dim_calendar as (
    select * from {{ ref('dim_calendar') }}
),

final_join as (
    select
        -- Surrogate Key
        i.sk_order_item, 
        
        -- Dimension Surrogate Keys
        cal.sk_time_version, 
        c.sk_customer_version, 
        r.sk_sales_representative_version, 
        col.sk_collection_version, 
        p.sk_product_version, 
        
        -- Degenerate Dimensions
        i.cd_order, 
        i.cd_product, 
        h.cd_customer, 
        h.cd_sales_representative, 
        i.cd_company, 
        
        -- Dates
        h.dt_issued, 
        
        -- Item Quantities
        i.qt_ordered, 
        i.qt_fulfilled, 
        i.qt_blocked, 
        i.qt_canceled, 
        
        -- Item Unit Prices
        i.vl_unit_price, 
        i.vl_net_unit_price, 
        i.vl_unit_price_me, 
        
        -- Item Total Financial Amounts
        i.vl_original_amount, 
        i.vl_billed_amount, 
        i.vl_canceled_amount, 
        i.vl_balance_amount, 
        i.vl_total_me,
        
        current_timestamp() as updated_at 

    from item_sales i
   
    inner join order_header h 
        on i.cd_order = h.cd_order
        
    left join dim_customers c
        on h.cd_customer = c.cd_customer
        and h.dt_issued between c.valid_from and coalesce(c.valid_to, '9999-12-31')

    left join dim_representatives r
        on h.cd_sales_representative = r.cd_sales_representative
        and h.dt_issued between r.valid_from and coalesce(r.valid_to, '9999-12-31')
        
    left join dim_collections col 
        on h.id_collection = col.id_collection
        
    left join dim_products p
        on i.cd_product = p.cd_product
        and h.dt_issued between p.valid_from and coalesce(p.valid_to, '9999-12-31')
        
    left join dim_calendar cal 
        on h.dt_issued = cal.dt_date
)

select * from final_join