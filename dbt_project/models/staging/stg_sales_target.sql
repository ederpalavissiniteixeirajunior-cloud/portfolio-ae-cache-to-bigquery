with source as (
    select * from {{ ref('seed_sales_target') }}
),

renamed as (
    select
        CAST(id_collection as int64) as id_collection,
        {{ convert_money('sales_target') }} as sales_target,
        cast(nm_group_sales_representative as string) as nm_group_sales_representative,
        {{ generate_audit_columns() }}

    from source
)

select * from renamed