with gps as (
    SELECT 
        g.* except(longitude, latitude),
        ST_GEOGPOINT(longitude, latitude) posicao_veiculo_geo,
        d.data_versao_efetiva_shapes data_versao
    FROM `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo` g
    JOIN `rj-smtr.br_rj_riodejaneiro_sigmob.data_versao_efetiva` d
    ON g.data = d.data
    WHERE g.data between '2022-02-15' and '2022-02-21'
),
shapes as (
    SELECT 
        *
    FROM `rj-smtr.br_rj_riodejaneiro_sigmob.shapes_geom`
    where 
        data_versao between (select min(data_versao) from gps) and (select max(data_versao) from gps)
    AND
        id_modal_smtr in ('22', 'O')
),
status_viagem as (
    SELECT
        id_veiculo,
        timestamp_gps,
        data,
        servico,
        distancia,
        shape_id,
        shape_distance,
        flag_trajeto_correto_hist,
        CASE
            WHEN ST_DWITHIN(posicao_veiculo_geo, start_pt, 200)
            THEN 'start'
            WHEN ST_DWITHIN(posicao_veiculo_geo, end_pt, 200)
            THEN 'end'
            WHEN flag_trajeto_correto_hist is true
            THEN 'middle'
        ELSE 'out'
        END status_viagem
    FROM gps g
    JOIN shapes s
    ON g.data_versao = s.data_versao
    AND g.servico = s.linha_gtfs
)
SELECT *
FROM status_viagem
-- ORDER BY id_veiculo, servico, shape_id