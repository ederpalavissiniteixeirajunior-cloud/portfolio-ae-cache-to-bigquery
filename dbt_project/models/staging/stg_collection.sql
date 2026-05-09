-- models/staging/stg_accounts_receivable.sql

with source as (
    select * from {{ ref('seed_collection') }}
),

renamed as (
    select
        CAST(id_collection as int64) as id_collection,
        cast(collection_name as string) as collection_name,
        parse_date('%d/%m/%Y', start_date) as start_date,
        parse_date('%d/%m/%Y', end_date) as end_date,
        {{ generate_audit_columns() }}

    from source
)

select * from renamed