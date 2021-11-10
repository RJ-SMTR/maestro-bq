SELECT
    data,
    extract(
        time from TIMESTAMP_SECONDS(
                {{ faixa_horaria_minutos }}*60 * DIV(
                    UNIX_SECONDS(cast(timestamp_gps as timestamp)),
                    {{ faixa_horaria_minutos }}*60)
                )
    ) faixa_horaria,
    servico,
    count(distinct id_veiculo) n_veiculos,
    count(id_veiculo) total_capturado,
    count(case when flag_trajeto_correto_hist is false then 1 end) n_fora_trajeto
FROM 
    {{ gps_sppo }} 
WHERE
    tipo_parada != 'garagem'
GROUP BY data, faixa_horaria, servico