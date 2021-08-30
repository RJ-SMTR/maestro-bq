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
      WHEN st_dwithin(shape, st_geogpoint(longitude, latitude), 100) THEN TRUE
    ELSE FALSE
    END AS flag_trajeto_correto,
    CASE
      WHEN COUNT(CASE
        WHEN st_dwithin(shape, st_geogpoint(longitude, latitude), 100) THEN 1 END) 
        OVER (PARTITION BY codigo ORDER BY UNIX_SECONDS(TIMESTAMP(timestamp_gps)) range between 600 preceding and current row) >= 1
      THEN true
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