with collection_targets as (
select 
        id_collection,
        sum(sales_target) as total_target
    from {{ ref('stg_sales_target') }}
    group by 1
),

collection_info as (
    select 
        id_collection,
        sk_collection_version,
        collection_name,
        min(start_date) as start_date,
        max(end_Date) as end_Date,
        date_diff(max(end_Date), min(start_date), day) +1 as total_days
    from {{ ref('dim_collection') }}
    group by 1,2,3
),

target_daily_base as (
    select 
        t.id_collection,
        t.total_target / c.total_days as daily_target_value,
        c.start_date,
        c.end_Date
    from collection_targets t
    inner join collection_info c on t.id_collection = c.id_collection
),

daily_series as (
    select 
        c.id_collection,
        c.sk_collection_version,
        c.collection_name,
        cal.dt_date
    from {{ ref('dim_calendar') }} cal
    cross join collection_info c 
    where cal.dt_date between c.start_date and c.end_Date
),

daily_sales as (
    select 
        f.dt_issued as sales_date,
        f.sk_collection_version,
        sum(f.vl_original_total) as daily_revenue
    from {{ ref('fct_orders') }} f
    group by 1, 2
),

final_cumulative as (
    select
        ds.dt_date as reference_date,
        ds.id_collection,
        ds.collection_name,
        sum(coalesce(v.daily_revenue, 0)) over (
            partition by ds.id_collection 
            order by ds.dt_date
            rows between unbounded preceding and current row
        ) as cumulative_sales,
        sum(t.daily_target_value) over (
            partition by ds.id_collection 
            order by ds.dt_date
            rows between unbounded preceding and current row
        ) as cumulative_target
    from daily_series ds
    left join daily_sales v 
        on ds.dt_date = v.sales_date 
        and ds.sk_collection_version = v.sk_collection_version
    inner join target_daily_base t 
        on ds.id_collection = t.id_collection
)

select * from final_cumulative