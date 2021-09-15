/*
Descrição:
Identifica veículos parados em terminais ou garagens conhecidas.
1. Selecionamos os terminais conhecidos e uma geometria do tipo polígono (Polygon) que contém buracos nas
localizações das garagens.
2. Calculamos as distâncias do veículos em relação aos terminais conhecidos. Definimos aqui a coluna 'nrow',
que identifica qual o terminal que está mais próximo do ponto informado. No passo final, recuperamos apenas 
os dados com nrow = 1 (menor distância em relação à posição do veículo)
3. Definimos uma distancia_limiar_parada. Caso o veículo esteja a uma distância menor que este valor de uma
parada, será considerado como parado no terminal com menor distancia.
4. Caso o veiculo não esteja intersectando o polígono das garagens, ele será considerado como parado dentro
de uma garagem (o polígono é vazado nas garagens, a não intersecção implica em estar dentro de um dos 'buracos').
*/
WITH 
  terminais AS (
    -- 1. Selecionamos terminais, criando uma geometria de ponto para cada.
    SELECT
      ST_GEOGPOINT(longitude, latitude) ponto_parada, nome_estacao nome_parada, 'terminal' tipo_parada
    FROM {{ terminais }}
  ),
  garagem_polygon AS (
    -- 1. Selecionamos o polígono das garagens.
    SELECT  ST_GEOGFROMTEXT(WKT,make_valid => true) AS poly
    FROM {{ polygon_garagem }}
  ),
  distancia AS (
    --2. Calculamos as distâncias e definimos nrow
    SELECT 
      timestamp_gps, posicao_veiculo_geo, id_veiculo, nome_parada, tipo_parada,
      ROUND(ST_DISTANCE(posicao_veiculo_geo, ponto_parada), 1) distancia_parada,
      ROW_NUMBER() OVER (PARTITION BY timestamp_gps, id_veiculo ORDER BY ST_DISTANCE(posicao_veiculo_geo, ponto_parada)) nrow
    FROM terminais p
    JOIN {{ registros_filtrada }} r
    ON 1=1
  )
SELECT
  * EXCEPT(nrow),
  /*
  3. e 4. Identificamos o status do veículo como 'terminal', 'garagem' (para os veículos parados) ou 
  'nao_identificado' (para os veículos mais distantes de uma parada que o limiar definido)
  */
  CASE
    WHEN distancia_parada < {{ distancia_limiar_parada }} THEN tipo_parada
    WHEN NOT ST_INTERSECTS(posicao_veiculo_geo, (SELECT  poly FROM garagem_polygon)) THEN 'garagem'
    ELSE 'nao_identificado'
  END status_tipo_parada,
FROM distancia
WHERE nrow = 1