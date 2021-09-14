-- calcula estado de movimento do carro em operação and test
WITH terminais as (
  select
    ST_GEOGPOINT(longitude, latitude) ponto_parada, nome_terminal nome_parada, 'terminal' tipo_parada
  from {{ terminais }}),
garagem_polygon AS (
/*3. Identifica registros que estão dentro de garagens conhecidas*/
	SELECT  ST_GEOGFROMTEXT(WKT,make_valid => true) AS poly
	FROM {{ polygon_garagem }}
),
distancia AS (
  SELECT 
    timestamp_gps, posicao_veiculo_geo, id_veiculo, nome_parada, tipo_parada,
    ROUND(ST_DISTANCE(posicao_veiculo_geo, ponto_parada), 1) distancia_parada,
    ROW_NUMBER() OVER (PARTITION BY timestamp_gps, id_veiculo ORDER BY ST_DISTANCE(posicao_veiculo_geo, ponto_parada)) nrow
  FROM terminais p
  join {{ registros_filtrada }} o
  on 1=1
  )
SELECT
  * except(nrow),
  case
    when distancia_parada < {{ distancia_limiar_parada }} then tipo_parada
    when not ST_INTERSECTS(posicao_veiculo_geo, (SELECT  poly FROM garagem_polygon)) then 'garagem'
    else 'nao_identificado'
  end status_tipo_parada,
FROM distancia
WHERE nrow = 1