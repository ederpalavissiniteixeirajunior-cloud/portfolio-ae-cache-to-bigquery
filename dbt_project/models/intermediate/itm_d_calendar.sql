{{ config(materialized='table') }}

WITH calendario AS (
  SELECT
    date_day
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2024-01-01', DATE_ADD(DATE_TRUNC(CURRENT_DATE(), YEAR), INTERVAL 6 YEAR), INTERVAL 1 DAY)) AS date_day

), intermediario AS (

  SELECT
    CAST(FORMAT_DATE('%Y%m%d', date_day) AS INT64)                           AS sk_time,
    date_day                                                                 AS dt_date,
    EXTRACT(YEAR FROM date_day)                                              AS nu_year,
    EXTRACT(MONTH FROM date_day)                                             AS nu_month,
    EXTRACT(DAY FROM date_day)                                               AS nu_day,
    EXTRACT(DAYOFYEAR FROM date_day)                                         AS nu_day_year,
    EXTRACT(DAYOFWEEK FROM date_day)                                         AS nu_day_week,
    EXTRACT(QUARTER FROM date_day)                                           AS nu_quarter,
    CASE WHEN EXTRACT(MONTH FROM date_day) <= 6 THEN 1 ELSE 2 END            AS nu_semester,
    CASE EXTRACT(MONTH FROM date_day)
      WHEN 1 THEN 'January'
      WHEN 2 THEN 'February'
      WHEN 3 THEN 'March'
      WHEN 4 THEN 'April'
      WHEN 5 THEN 'May'
      WHEN 6 THEN 'June'
      WHEN 7 THEN 'July'
      WHEN 8 THEN 'August'
      WHEN 9 THEN 'September'
      WHEN 10 THEN 'October'
      WHEN 11 THEN 'November'
      WHEN 12 THEN 'December'
    END                                                                      AS ds_month,
    CASE EXTRACT(MONTH FROM date_day)
      WHEN 1 THEN 'Jan'
      WHEN 2 THEN 'Feb'
      WHEN 3 THEN 'Mar'
      WHEN 4 THEN 'Apr'
      WHEN 5 THEN 'May'
      WHEN 6 THEN 'Jun'
      WHEN 7 THEN 'Jul'
      WHEN 8 THEN 'Aug'
      WHEN 9 THEN 'Sep'
      WHEN 10 THEN 'Oct'
      WHEN 11 THEN 'Nov'
      WHEN 12 THEN 'Dec'
    END                                                                      AS ds_month_abbreviated,
    CASE EXTRACT(DAYOFWEEK FROM date_day)
      WHEN 1 THEN 'Sunday'
      WHEN 2 THEN 'Monday'
      WHEN 3 THEN 'Tuesday'
      WHEN 4 THEN 'Wednesday'
      WHEN 5 THEN 'Thursday'
      WHEN 6 THEN 'Friday'
      WHEN 7 THEN 'Saturday'
    END                                                                      AS ds_day_week,
    CASE EXTRACT(DAYOFWEEK FROM date_day)
      WHEN 1 THEN 'Sun'
      WHEN 2 THEN 'Mon'
      WHEN 3 THEN 'Tue'
      WHEN 4 THEN 'Wed'
      WHEN 5 THEN 'Thu'
      WHEN 6 THEN 'Fri'
      WHEN 7 THEN 'Sat'
    END                                                                      AS ds_day_week_abbreviated,
    CONCAT(CAST(EXTRACT(QUARTER FROM date_day) AS STRING), 'º Quarter')    AS ds_quarter,
    CASE WHEN EXTRACT(MONTH FROM date_day) <= 6 THEN '1º Quarter' ELSE '2º Quarter' END AS ds_semester,    
    CASE WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE ELSE FALSE END AS in_weekend,
    CASE WHEN EXTRACT(DAYOFWEEK FROM date_day) NOT IN (1, 7) THEN TRUE ELSE FALSE END AS in_business_day,
    CURRENT_TIMESTAMP()                                                      AS updated_at

  FROM calendario

)

SELECT * FROM intermediario