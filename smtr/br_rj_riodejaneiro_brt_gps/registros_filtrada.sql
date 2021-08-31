WITH box AS (
  SELECT
    *
  FROM {{ limites_caixa }}
)
SELECT 
  codigo as id_veiculo, 	
  placa as placa_veiculo, 	
  linha, 	
  latitude, 	
  longitude, 	
  timestamp_gps, 	
  velocidade, 	
  id_migracao_trajeto, 	
  sentido, 	
  trajeto, 	
  timestamp_captura, 	
  data, 	
  hora,
  STRUCT({{ maestro_sha }} AS versao_maestro, {{ maestro_bq_sha }} AS versao_maestro_bq) versao 	
FROM {{ registros }} 
WHERE data BETWEEN DATE({{ date_range_start }}) AND DATE({{ date_range_end }})  
AND DATETIME_DIFF(timestamp_captura, timestamp_gps, MINUTE) < 2
AND longitude BETWEEN (SELECT min_longitude FROM box) AND (SELECT max_longitude FROM box)
AND latitude BETWEEN (SELECT min_latitude FROM box) AND (SELECT max_latitude FROM box)
