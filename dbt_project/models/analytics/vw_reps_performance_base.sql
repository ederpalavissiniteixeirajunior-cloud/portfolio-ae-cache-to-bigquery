select
    col.collection_name,
    rep.nm_group_sales_representative as representative,
    sum(f.vl_original_total) as total_sales
from {{ ref('fct_orders') }} f
join {{ ref('dim_collection') }} col on f.sk_collection_version = col.sk_collection_version
join {{ ref('dim_sales_representative') }} rep on f.sk_sales_representative_version = rep.sk_sales_representative_version
where nm_group_sales_representative <> "-"
group by 1, 2
order by 3 desc
