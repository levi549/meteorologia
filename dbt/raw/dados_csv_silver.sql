with dados_raw_csv as (
    select city_id,city_name,dt,temp,humidity,pressure,weather_main,anomaly_name from {{source('fonte_supabase','raw_csv')}}
)




select 
{{dbt_utils.generate_surrogate_key(['city_id','city_name'])}} as id, 
 city_name,
 SIN(EXTRACT(MONTH FROM dt::DATE)*2*PI()/12) AS mes_sin,
 COS(EXTRACT(MONTH FROM dt::DATE)*2*PI()/12) AS mes_cos,
 (temp - AVG(temp) OVER()) / STDDEV(temp) OVER() as temp_padronizado,
 (humidity- AVG(humidity) OVER())/ STDDEV(humidity) OVER() as humidity_padronizado,
 (pressure-AVG(pressure)OVER())/ STDDEV(pressure) OVER() as pressure_padronizado,
 weather_main,
 anomaly_name
from dados_raw_csv


