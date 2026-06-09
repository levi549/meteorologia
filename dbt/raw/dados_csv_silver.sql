{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='fail',
    incremental_strategy='merge'
) }}

with dados_raw_csv as (
    select city_id,city_name,dt,temp,humidity,pressure,weather_main,anomaly_name from {{source('fonte_supabase','raw_csv')}}
    where dt is not null
    {% if is_incremental() %}
        and dt > (select max(dt) from {{ this }})
    {% endif %}
    
),


medianas_cidade as (
    select
        city_id,
        percentile_cont(0.5) within group (order by temp) as temp_mediana,
        percentile_cont(0.5) within group (order by humidity) as humidity_mediana,
        percentile_cont(0.5) within group (order by pressure) as pressure_mediana
    from {{ source('fonte_supabase', 'raw_csv') }}
    group by city_id
),
 dados_padronizados as (
    select  
        r.city_id,
        r.city_name,
        r.dt,
        r.temp,
        r.humidity,
        r.pressure,
        r.weather_main,
        r.anomaly_name,
        sin(extract(month from r.dt::date) * 2 * pi() / 12) as mes_sin,
        cos(extract(month from r.dt::date) * 2 * pi() / 12) as mes_cos,
        m.temp_mediana,
        m.humidity_mediana,
        m.pressure_mediana
    from dados_raw_csv r
    left join medianas_cidade m on r.city_id = m.city_id
    
)


SELECT 
    {{ dbt_utils.generate_surrogate_key(['city_id', 'dt']) }} AS id,
    city_name,
    mes_sin,
    mes_cos,
    COALESCE(temp, temp_mediana) AS temp,
    COALESCE(humidity, humidity_mediana) AS humidity,
    COALESCE(pressure, pressure_mediana) AS pressure,
    weather_main,
    anomaly_name
FROM dados_padronizados