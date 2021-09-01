-- Join dos passos de tratamento and test
WITH
  velocidades AS (
  SELECT
    * except(versao)
  FROM
    {{ velocidade_status }} 
  WHERE data BETWEEN DATE({{ date_range_start }}) AND DATE({{ date_range_end }})
  ),
  flags AS (
  SELECT
    * except(versao)
  FROM
    {{ tratamento_flag_trajeto_correto }}
  WHERE data BETWEEN DATE({{ date_range_start }}) AND DATE({{ date_range_end }})  
  )
SELECT
  v.*,
  flag_trajeto_correto,
  flag_trajeto_correto_hist,
  STRUCT({{ maestro_sha }} AS versao_maestro, {{ maestro_bq_sha }} AS versao_maestro_bq) versao
FROM
  velocidades v
JOIN
  flags f
ON
  v.id_veiculo = f.id_veiculo
  AND v.linha = f.linha
  AND v.timestamp_captura = f.timestamp_captura
