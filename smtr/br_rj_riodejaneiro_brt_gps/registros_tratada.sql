WITH
  velocidades AS (
  SELECT
    *
  FROM
    {{ tratamento_velocidade }} 
  where data between DATE({{ date_range_start }}) and DATE({{ date_range_end }})
  ),
  flags AS (
  SELECT
    *
  FROM
    {{ tratamento_flag_trajeto_correto }}
  where data between DATE({{ date_range_start }}) and DATE({{ date_range_end }})  
  )
SELECT
  v.*,
  count_compliance,
  flag_trajeto_correto
FROM
  velocidades v
JOIN
  flags f
ON
  v.id_veiculo = f.id_veiculo
  AND v.linha = f.linha
  AND v.timestamp_gps = f.timestamp_gps
