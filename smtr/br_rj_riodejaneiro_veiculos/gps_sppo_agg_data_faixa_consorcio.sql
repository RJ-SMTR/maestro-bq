with 
consorcios as (
    select
        data, 
        consorcio,
        route_short_name,
    from ( 
        SELECT 
            r1.*,
            data,
            data_versao_efetiva_agency
        FROM 
            {{ routes }} r1
        JOIN
            {{ data_versao_efetiva }} d
        ON
            r1.data_versao = d.data_versao_efetiva_routes
    ) r
    join (
        select 
            agency_id,
            Normalize_and_Casefold(json_value(content, "$.agency_name")) consorcio,
            DATE(data_versao) as data_versao
        from {{ agency }}
    ) c
    on
        r.agency_id = c.agency_id
    and
        r.data_versao_efetiva_agency = c.data_versao
)
SELECT
    t1.data,
    extract(
        time from TIMESTAMP_SECONDS(
                {{ faixa_horaria_minutos }}*60 * DIV(
                    UNIX_SECONDS(cast(timestamp_gps as timestamp)),
                    {{ faixa_horaria_minutos }}*60)
                )
    ) faixa_horaria,
    consorcio,
    count(distinct id_veiculo) n_veiculos,
    count(id_veiculo) total_capturado,
    count(case when flag_trajeto_correto_hist is false then 1 end) n_fora_trajeto
FROM 
    {{ gps_sppo }} t1
JOIN 
    consorcios t2
ON
    t1.data = t2.data
    and t1.servico = t2.route_short_name
WHERE
    tipo_parada != 'garagem'
GROUP BY data, faixa_horaria, consorcio