with dados_raw_csv as (
    select city_name,dt,temp,humidity,pressure,weather_main,anomaly_name from {{source('fonte_supabase','raw_csv')}}
)


with dados_padronizados as(
    select 
)



select 
 city_name,
 dt::DATE as data,
 EXTRACT(MONTH FROM dt::DATE) AS numero_mes,
 temp,
 humidity,
 pressure,
 weather_main,
 anomaly_name
from dados_raw_csv


