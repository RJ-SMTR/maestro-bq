WITH
  registros AS (
  SELECT
    distinct *,
  FROM
    `rj-smtr-dev.br_rj_riodejaneiro_brt_gps.registros_tratada_1_dia` r ),
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
    `rj-smtr-dev.br_rj_riodejaneiro_sigmob.materialized_shapes_geom` s
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
order by codigo, timestamp_gps