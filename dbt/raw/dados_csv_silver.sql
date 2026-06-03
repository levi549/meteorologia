with dados_raw_csv as (
    select city_id,city_name,dt,temp,humidity,pressure,weather_main,anomaly_name from {{source('fonte_supabase','raw_csv')}}
),

 dados_padronizados as (
    select 
        city_id,
        city_name,
        dt,
        temp,
        humidity,
        pressure,
        weather_main,
        anomaly_name,
        SIN(EXTRACT(MONTH FROM dt::DATE) * 2 * PI() / 12) AS mes_sin,
        COS(EXTRACT(MONTH FROM dt::DATE) * 2 * PI() / 12) AS mes_cos,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY temp) OVER(PARTITION BY city_id) AS temp_mediana,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY humidity) OVER(PARTITION BY city_id) AS humidity_mediana,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pressure) OVER(PARTITION BY city_id) AS pressure_mediana
    from dados_raw_csv
)


SELECT 
    {{ dbt_utils.generate_surrogate_key(['city_id', 'dt']) }} AS id,
    id_referencia BIGINT GENERATED ALWAYS AS IDENTITY ,
    city_name,
    mes_sin,
    mes_cos,
    COALESCE(temp, temp_mediana) AS temp,
    COALESCE(humidity, humidity_mediana) AS humidity,
    COALESCE(pressure, pressure_mediana) AS pressure,
    weather_main,
    anomaly_name
FROM dados_padronizados