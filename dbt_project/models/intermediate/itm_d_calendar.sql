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
      WHEN 1 THEN 'Janeiro'
      WHEN 2 THEN 'Fevereiro'
      WHEN 3 THEN 'Março'
      WHEN 4 THEN 'Abril'
      WHEN 5 THEN 'Maio'
      WHEN 6 THEN 'Junho'
      WHEN 7 THEN 'Julho'
      WHEN 8 THEN 'Agosto'
      WHEN 9 THEN 'Setembro'
      WHEN 10 THEN 'Outubro'
      WHEN 11 THEN 'Novembro'
      WHEN 12 THEN 'Dezembro'
    END                                                                      AS ds_month,
    CASE EXTRACT(MONTH FROM date_day)
      WHEN 1 THEN 'Jan'
      WHEN 2 THEN 'Fev'
      WHEN 3 THEN 'Mar'
      WHEN 4 THEN 'Abr'
      WHEN 5 THEN 'Mai'
      WHEN 6 THEN 'Jun'
      WHEN 7 THEN 'Jul'
      WHEN 8 THEN 'Ago'
      WHEN 9 THEN 'Set'
      WHEN 10 THEN 'Out'
      WHEN 11 THEN 'Nov'
      WHEN 12 THEN 'Dez'
    END                                                                      AS ds_month_abbreviated,
    CASE EXTRACT(DAYOFWEEK FROM date_day)
      WHEN 1 THEN 'Domingo'
      WHEN 2 THEN 'Segunda-feira'
      WHEN 3 THEN 'Terça-feira'
      WHEN 4 THEN 'Quarta-feira'
      WHEN 5 THEN 'Quinta-feira'
      WHEN 6 THEN 'Sexta-feira'
      WHEN 7 THEN 'Sábado'
    END                                                                      AS ds_day_week,
    CASE EXTRACT(DAYOFWEEK FROM date_day)
      WHEN 1 THEN 'Dom'
      WHEN 2 THEN 'Seg'
      WHEN 3 THEN 'Ter'
      WHEN 4 THEN 'Qua'
      WHEN 5 THEN 'Qui'
      WHEN 6 THEN 'Sex'
      WHEN 7 THEN 'Sáb'
    END                                                                      AS ds_day_week_abbreviated,
    CONCAT(CAST(EXTRACT(QUARTER FROM date_day) AS STRING), 'º Trimestre')    AS ds_quarter,
    CASE WHEN EXTRACT(MONTH FROM date_day) <= 6 THEN '1º Semestre' ELSE '2º Semestre' END AS ds_semester,    
    CASE WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE ELSE FALSE END AS in_weekend,
    CASE WHEN EXTRACT(DAYOFWEEK FROM date_day) NOT IN (1, 7) THEN TRUE ELSE FALSE END AS in_business_day,
    CURRENT_TIMESTAMP()                                                      AS updated_at

  FROM calendario

)

SELECT * FROM intermediario