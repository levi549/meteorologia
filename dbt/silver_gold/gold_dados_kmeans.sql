{{ config(
    materialized='incremental',
    unique_key='id',
)}}

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
),


dados_padronizados as (
    select 
    id,
    mes_sin,
    mes_cos,
    case 
        when stddev(temp) OVER( PARTITION BY city_name)=0 then 0
        else (temp - avg(temp) OVER( PARTITION BY city_name)) / NULLIF(stddev(temp) OVER(PARTITION BY city_name), 0)
    end as temp_padronizado,
    case 
        when stddev(humidity) OVER( PARTITION BY city_name)=0 then 0
        else (humidity - avg(humidity) OVER( PARTITION BY city_name)) / NULLIF(stddev(humidity) OVER(PARTITION BY city_name), 0)
    end as humidity_padronizado,
    case 
        when stddev(pressure) OVER( PARTITION BY city_name)=0 then 0
        else (pressure - avg(pressure) OVER( PARTITION BY city_name)) / NULLIF(stddev(pressure) OVER(PARTITION BY city_name), 0)
    end as pressure_padronizada,
    anomaly_name
    from dados_csv_silver
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