with source as (
    select * from {{ ref('itm_d_calendar') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['sk_time']) }} as sk_time_version,
        sk_time, 
        dt_date,
        nu_year,
        nu_month,
        nu_day,
        nu_day_year,
        nu_day_week,
        nu_quarter,
        nu_semester,
        ds_month,
        ds_month_abbreviated,
        ds_day_week,
        ds_day_week_abbreviated,
        ds_quarter,
        ds_semester,
        in_weekend,
        in_business_day,
        current_timestamp() as updated_at
        from source
)

select * from final