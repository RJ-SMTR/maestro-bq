WITH gps AS (
  SELECT *, 
    REGEXP_REPLACE(SPLIT(trajeto, ' ')[SAFE_OFFSET(0)], '[^a-zA-Z0-9]', '') linha_trajeto
  FROM {{ registros_filtrada }}
  where data between DATE({{ date_range_start }}) and DATE({{ date_range_end }})  
)
SELECT 
  t.*,
  extract(time from t.timestamp_gps) as hora_completa,
  linha_trajeto = linha flag_linha_similar_trajeto,
  t2.velocidade as velocidade_estimada_10_min,
  t2.nome_parada,
  t2.tipo_parada,
  t2.distancia_parada,
  t2.status_movimento,
  t2.status_tipo_parada,
  STRUCT({{ maestro_sha }} AS versao_maestro, {{ maestro_bq_sha }} AS versao_maestro_bq) versao
FROM gps t
JOIN {{ velocidade_status }} t2
ON t.timestamp_captura = t2.timestamp_captura
AND t.placa_veiculo = t2.placa_veiculo