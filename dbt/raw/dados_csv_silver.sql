with dados_raw_csv as (
    select city_id,city_name,dt,temp,humidity,pressure,weather_main,anomaly_name from {{source('fonte_supabase','raw_csv')}}
)




SELECT 
    {{ dbt_utils.generate_surrogate_key(['city_id', 'city_name']) }} AS id, 
    city_id,
    city_name,
    SIN(EXTRACT(MONTH FROM dt::DATE) * 2 * PI() / 12) AS mes_sin,
    COS(EXTRACT(MONTH FROM dt::DATE) * 2 * PI() / 12) AS mes_cos,
    (temp - AVG(temp) OVER(PARTITION BY city_id)) / 
        NULLIF(STDDEV(temp) OVER(PARTITION BY city_id), 0) AS temp_padronizado,     
    (humidity - AVG(humidity) OVER(PARTITION BY city_id)) / 
        NULLIF(STDDEV(humidity) OVER(PARTITION BY city_id), 0) AS humidity_padronizado,
    (pressure - AVG(pressure) OVER(PARTITION BY city_id)) / 
        NULLIF(STDDEV(pressure) OVER(PARTITION BY city_id), 0) AS pressure_padronizado,
    weather_main,
    anomaly_name
FROM dados_raw_csv