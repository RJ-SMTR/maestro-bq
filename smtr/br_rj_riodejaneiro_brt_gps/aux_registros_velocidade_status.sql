WITH paradas as (
  select
    ST_GEOGPOINT(longitude, latitude) ponto_parada, nome_estacao nome_parada, 'estacao' tipo_parada
  from {{ estacoes }} t1
  union all
  select
    ST_GEOGPOINT(longitude, latitude) ponto_parada, nome_empresa nome_parada, 'garagem' tipo_parada
  from {{ garagens }} t2
  where ativa = 1),
onibus_parados AS (
  select
    *, ST_GEOGPOINT(longitude, latitude) ponto_carro
  from {{ velocidade_carro }} 
  where data between DATE({{ date_range_start }}) and DATE({{ date_range_end }})   
  ),
distancia AS (
  SELECT 
    timestamp_captura, velocidade, placa_veiculo, longitude, latitude, nome_parada, tipo_parada,
    ST_DISTANCE(ponto_carro, ponto_parada) distancia_parada, 
    ROW_NUMBER() OVER (PARTITION BY timestamp_captura, placa_veiculo ORDER BY ST_DISTANCE(ponto_carro, ponto_parada)) nrow
  FROM paradas p
  join onibus_parados o
  on 1=1
  )
SELECT
  * except(nrow),
  case
    when velocidade < 3 then 'parado'
    else 'andando'
  end status_movimento,
  case
    when distancia_parada < 1000 then tipo_parada
    else 'nao_identificado'
  end status_tipo_parada
FROM distancia
WHERE nrow = 1