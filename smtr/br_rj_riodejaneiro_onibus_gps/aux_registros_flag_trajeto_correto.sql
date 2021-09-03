-- intersec route shapes and car points
WITH
  registros AS (
  SELECT id_veiculo, linha, latitude, longitude, data, timestamp_gps, timestamp_captura
  FROM
  {{ registros_filtrada }} r 
  ),
counts AS (
SELECT
  *,
  CASE
    WHEN st_dwithin(shape, st_geogpoint(longitude, latitude), {{ tamanho_buffer_metros }}) THEN TRUE
  ELSE FALSE
  END AS flag_trajeto_correto,
  CASE
    WHEN COUNT(CASE
      WHEN st_dwithin(shape, st_geogpoint(longitude, latitude), {{ tamanho_buffer_metros }}) THEN 1 END) 
      OVER (PARTITION BY id_veiculo 
            ORDER BY UNIX_SECONDS(TIMESTAMP(timestamp_gps)) 
            RANGE BETWEEN {{ intervalo_max_desvio_segundos }} PRECEDING AND CURRENT ROW) >= 1
    THEN True
    ELSE False
  END AS flag_trajeto_correto_hist
FROM (
  SELECT * 
  FROM {{ shapes }}
  WHERE id_modal_smtr in ('22', '23')
) s
JOIN
  registros r
ON
  r.linha = s.linha_gtfs
AND
  r.data = s.data_versao
)
SELECT
  id_veiculo,
  linha,
  linha_gtfs,
  data,
  timestamp_gps,
  timestamp_captura,
  flag_trajeto_correto,
  flag_trajeto_correto_hist,
  STRUCT({{ maestro_sha }} AS versao_maestro, {{ maestro_bq_sha }} AS versao_maestro_bq) versao
FROM
  counts c