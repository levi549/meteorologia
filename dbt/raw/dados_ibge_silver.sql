with json_ibge as(
    select dados_json_ibge from {{source('fonte_supabase','raw_ibge')}}
)
select 
id BIGINT GENERATED ALWAYS AS IDENTITY,
(dados_json_ibge -> 0 -> 'resultados' -> 0 -> 'series' -> 0 -> 'serie' ->> '2021')::INTEGER AS populacao
from json_ibge
