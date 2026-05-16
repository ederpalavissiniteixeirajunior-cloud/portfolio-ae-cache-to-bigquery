with staging as (
    select * from {{ ref('stg_orders') }}
    where cd_company = 1
      and nm_operator LIKE 'Integração GEOvendas%'
),
collections as (
    select * from {{ ref('stg_collection') }}
),

intermediate as (
    select
        {{ dbt_utils.generate_surrogate_key(['s.cd_order']) }} as sk_order_version,
        s.cd_order,
        s.cd_customer,
        s.cd_sales_representative,
        s.vl_original_total,
        s.dt_issued,
        s.nm_status,
        c.id_collection,
        current_timestamp() as updated_at
    from staging s
    inner join collections c
        on s.dt_issued >= c.start_date
        and (s.dt_issued <= c.end_date OR c.end_date IS NULL)
)

select * from intermediate
where id_collection IS NOT NULL