WITH
  velocidades AS (
  SELECT
    *
  FROM
    {{ tratamento_velocidade }} 
  WHERE data BETWEEN DATE({{ date_range_start }}) AND DATE({{ date_range_end }})
  ),
  flags AS (
  SELECT
    *
  FROM
    {{ tratamento_flag_trajeto_correto }}
  WHERE data BETWEEN DATE({{ date_range_start }}) AND DATE({{ date_range_end }})  
  )
SELECT
  v.*,
  count_compliance,
  flag_trajeto_correto,
  STRUCT({{ maestro_sha }} AS versao_maestro, {{ maestro_bq_sha }} AS versao_maestro_bq) versao
FROM
  velocidades v
JOIN
  flags f
ON
  v.id_veiculo = f.id_veiculo
  AND v.linha = f.linha
  AND v.timestamp_gps = f.timestamp_gps
