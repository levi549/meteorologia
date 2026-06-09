{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='fail',
    incremental_strategy='merge'
) }}

with dados_csv_silver as(
    select
        id,
        city_name,
        mes_sin,
        mes_cos,
        temp,
        humidity,
        pressure,
        anomaly_name
        from {{ref('dados_csv_silver')}}
    {% if is_incremental() %}
      where id not in (select id from {{ this }})
    {% endif %}
),

estatisticas_cidade as (
    select
        city_name,
        avg(temp) as temp_media,
        NULLIF(stddev(temp), 0) as temp_desvio_padrao,
        avg(humidity) as humidity_media,
        NULLIF(stddev(humidity), 0) as humidity_desvio_padrao,
        avg(pressure) as pressure_media,
        NULLIF(stddev(pressure), 0) as pressure_desvio_padrao
    from {{ref('dados_csv_silver')}}
    group by city_name
),

dados_padronizados as (
    select 
    d.id,
    d.mes_sin,
    d.mes_cos,
    case 
        when e.temp_desvio_padrao=0 then 0
        else (d.temp - e.temp_media) / NULLIF(e.temp_desvio_padrao, 0)
    end as temp_padronizado,
    case 
        when e.humidity_desvio_padrao=0 then 0
        else (d.humidity - e.humidity_media) / NULLIF(e.humidity_desvio_padrao, 0)
    end as humidity_padronizado,
    case 
        when e.pressure_desvio_padrao=0 then 0
        else (d.pressure - e.pressure_media) / NULLIF(e.pressure_desvio_padrao, 0)
    end as pressure_padronizada,
    d.anomaly_name
    from dados_csv_silver d
    LEFT JOIN estatisticas_cidade e using (city_name)
    )


select
    id,
    mes_sin,
    mes_cos,
    temp_padronizado,
    humidity_padronizado,
    pressure_padronizada,
    anomaly_name
from dados_padronizados