WITH
  velocidades AS (
  SELECT
    *
  FROM
    `rj-smtr-dev.br_rj_riodejaneiro_brt_gps.registros_tratamento_velocidade` ),
  flags AS (
  SELECT
    *
  FROM
    `rj-smtr-dev.br_rj_riodejaneiro_brt_gps.registros_flag_trajeto_correto` )
SELECT
  v.*,
  count_compliance,
  flag_trajeto_correto
FROM
  velocidades v
JOIN
  flags f
ON
  v.codigo = f.codigo
  AND v.linha = f.linha
  AND v.timestamp_gps = f.timestamp_gps
ORDER BY
  codigo,
  linha,
  timestamp_gps