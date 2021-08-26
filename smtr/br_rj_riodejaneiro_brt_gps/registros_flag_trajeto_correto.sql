WITH
  counts AS (
  SELECT
    *,
    COUNT(
      CASE
        WHEN st_dwithin(shape, st_geogpoint(longitude, latitude), 100) THEN 1
    END
      ) OVER (PARTITION BY codigo ORDER BY timestamp_gps ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS count_compliance,
    COUNT(timestamp_gps) OVER (PARTITION BY codigo ORDER BY timestamp_gps ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS total_count
  FROM
    `rj-smtr-dev.br_rj_riodejaneiro_brt_gps.materialized_registros_tratada` r
  JOIN
    `rj-smtr-dev.br_rj_riodejaneiro_sigmob.materialized_shapes_geom` s
  ON
    r.linha = s.linha_gtfs
  WHERE
    DATE(timestamp_gps) = "2021-08-01" )
SELECT
  codigo,
  linha,
  linha_gtfs,
  timestamp_gps,
  count_compliance,
  total_count,
  CASE
    WHEN total_count >=5 AND count_compliance = 5 THEN TRUE
    WHEN total_count < 5 AND count_compliance >= total_count THEN TRUE
    ELSE FALSE
END
  AS flag_trajeto_correto
FROM
  counts c