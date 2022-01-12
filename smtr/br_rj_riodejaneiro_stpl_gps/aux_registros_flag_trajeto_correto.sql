with gps as (
  SELECT 
      id_veiculo,
      placa,
      replace(linha, "L", "STPL") linha,
      ST_GEOGPOINT(longitude, latitude) posicao_veiculo_geo,
      timestamp_gps,
      timestamp_captura,
      data
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
     CASE WHEN linha_gtfs is null THEN false ELSE true END flag_linha_existe_sigmob
  FROM (
    SELECT g1.*, data_versao_efetiva_shapes as data_versao_efetiva 
    FROM gps g1
    JOIN {{ data_versao_efetiva }} d
    ON d.data = g1.data
    ) g
  LEFT JOIN shapes s
  ON g.data_versao_efetiva = s.data_versao
  AND g.linha = s.linha_gtfs
)
SELECT * from flag