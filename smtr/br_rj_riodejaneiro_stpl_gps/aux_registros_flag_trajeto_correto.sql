with gps as (
  SELECT 
      data,
      id_veiculo,
      timestamp_gps,
      servico,
      rp,
      ST_GEOGPOINT(longitude, latitude) posicao_veiculo_geo,
  FROM {{ registros_filtrada }}
  WHERE data between DATE({{ date_range_start }}) and DATE({{ date_range_end }})
  AND timestamp_gps > {{ date_range_start }} AND timestamp_gps <= {{ date_range_end }}
),
shapes as (
 SELECT 
    trip_id,
    shape_id,
    linha_gtfs,
    shape,
    data_versao
 FROM {{ shapes }}
 WHERE linha_gtfs like 'STPL%' 
),
ap as (
  SELECT
    CASE
      WHEN
        regiao_ap = '4'
      THEN '41'
    ELSE
      REPLACE(regiao_ap,".", "")
    END regiao_ap,
    geometry as rp_geom
  FROM rj-smtr-dev.br_rj_riodejaneiro_rdo.regioes_planejamento
),
flag as (
  SELECT 
      g.*,
      linha_gtfs,
      CASE
        WHEN ST_DWITHIN(posicao_veiculo_geo, shape, {{ tamanho_buffer_metros }})
        THEN true
        ELSE false
      END flag_trajeto_correto,
      CASE
        WHEN 
          COUNT(CASE WHEN ST_DWITHIN(posicao_veiculo_geo, shape, {{ tamanho_buffer_metros }}) THEN 1 END) OVER(
          partition by id_veiculo
          order by UNIX_SECONDS(TIMESTAMP(timestamp_gps, 'America/Sao_Paulo'))
          range between {{ intervalo_max_desvio_segundos }} preceding and current row
          )>=1
        THEN true
        ELSE false
      END flag_trajeto_correto_hist,
    CASE WHEN linha_gtfs is null THEN false ELSE true END flag_linha_existe_sigmob,
    CASE
      WHEN
        ST_INTERSECTS(posicao_veiculo_geo, rp_geom)
      THEN
        true
    ELSE
      false
    END flag_ap_correta
  FROM (
    SELECT g1.*, data_versao_efetiva_shapes as data_versao_efetiva 
    FROM gps g1
    JOIN {{ data_versao_efetiva }} d
    ON d.data = g1.data
    ) g
  LEFT JOIN shapes s
  ON g.data_versao_efetiva = s.data_versao
  AND g.servico = s.linha_gtfs
  JOIN ap 
  ON g.rp = ap.regiao_ap
)
SELECT * from flag