with staging as (
    select * from {{ ref('stg_order_items') }}
),

simplified as (
    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['cd_order', 'cd_product']) }} as sk_order_item,
        
        -- Business Keys
        cd_order,
        cd_product,
        cd_company,
        
        -- Analytical Quantities
        qt_ordered,
        qt_fulfilled,
        qt_blocked,
        qt_canceled,
        
        -- Metric Unit Prices
        vl_unit_price,
        vl_net_unit_price,
        vl_unit_price_me,
        
        -- Metric Values
        vl_original_amount,
        vl_billed_amount,
        vl_canceled_amount,
        vl_balance_amount,
        vl_total_me
        
    from staging
)

select * from simplified
where cd_company = 1