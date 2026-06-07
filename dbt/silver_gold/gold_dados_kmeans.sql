with dados_csv_silver as(
    select
        id,
        id_referencia,
        mes_sin,
        mes_cos,
        temp,
        humidity,
        pressure
        from {{ref('dados_csv_silver')}}
)


