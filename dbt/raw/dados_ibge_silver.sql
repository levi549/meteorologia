WITH dados AS (
    SELECT dados_json_ibge FROM {{ source('fonte_supabase', 'raw_ibge') }}
),
dados_filtrados AS (
    SELECT 
        jsonb_path_query(dados_json_ibge, '$[*].resultados[*].series[*]') AS retorno
    FROM dados
)
SELECT 
    (retorno->'localidade'->>'nome') AS nome,          
    trim(both '"' from ano.value::text)::int AS populacao,
    CURRENT_TIMESTAMP AS ingested_at 
FROM dados_filtrados
CROSS JOIN LATERAL jsonb_each(retorno->'serie') AS ano