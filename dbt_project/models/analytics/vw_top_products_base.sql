select
    col.collection_name,
    p.ds_product_reference as product,
    sum(f.qt_ordered) as total_quantity,
    sum(f.vl_original_amount) as total_revenue
from {{ ref('fct_order_items') }} f
left join {{ ref('dim_collection') }} col
    on f.sk_collection_version = col.sk_collection_version
left join {{ ref('dim_products') }} p
    on f.cd_product = p.cd_product
    and p.is_current = true
where ds_product_reference IS NOT NULL
group by 1, 2
order by total_revenue desc