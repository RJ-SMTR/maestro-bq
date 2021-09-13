-- calcula estado de movimento do carro em operação and test
WITH paradas as (
  select
    ST_GEOGPOINT(longitude, latitude) ponto_parada, nome_terminal nome_parada, 'terminal' tipo_parada
  from {{ terminais }} t1
  union all
  select
    ST_GEOGPOINT(longitude, latitude) ponto_parada, nome_empresa nome_parada, 'garagem' tipo_parada
  from {{ garagens }} t2
  where ativa = 1),
distancia AS (
  SELECT 
    data, timestamp_captura, velocidade, id_veiculo, linha, longitude, latitude, nome_parada, tipo_parada,
    ROUND(ST_DISTANCE(ponto_carro, ponto_parada), 1) distancia_parada, versao,
    ROW_NUMBER() OVER (PARTITION BY timestamp_captura, id_veiculo ORDER BY ST_DISTANCE(ponto_carro, ponto_parada)) nrow
  FROM paradas p
  join {{ registros_filtrada }} o
  on 1=1
  )
SELECT
  * except(nrow),
  case
    when distancia_parada < {{ distancia_limiar }} then tipo_parada
    else 'nao_identificado'
  end status_tipo_parada,
FROM distancia
WHERE nrow = 1