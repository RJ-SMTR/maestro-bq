WITH
  registros AS (
  SELECT *
  FROM
  {{ registros_filtrada }} r 
  where data between DATE({{ date_range_start }}) and DATE({{ date_range_end }}) 
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
              RANGE BETWEEN {{ intervalo_max_desvio_minutos }}*60 PRECEDING AND CURRENT ROW) >= 1
      THEN True
      ELSE False
    END AS flag_trajeto_correto_hist
  FROM
    registros r
  JOIN
    {{ shapes }} s
  ON
    r.linha = s.linha_gtfs
)
SELECT
  codigo,
  linha,
  linha_gtfs,
  timestamp_gps,
  timestamp_captura,
  flag_trajeto_correto,
  flag_trajeto_correto_hist
FROM
  counts c