/*
Descrição:
Filtragem e tratamento básico de registros de gps.
1. Filtra registros que estão fora de uma caixa que contém a área do município de Rio de Janeiro.
2. Filtra registros antigos. Remove registros que tem diferença maior que 1 minuto entre o timestamp_captura e timestamp_gps.
3. Identifica registros que estão dentro de garagens conhecidas
4. Muda o nome de variáveis para o padrão do projeto.
	- id_veiculo --> ordem
	- hora_completa
*/
WITH garagem_polygon AS
/*3. Identifica registros que estão dentro de garagens conhecidas*/
(
	SELECT  ST_GEOGFROMTEXT(WKT,make_valid => true) AS poly
	FROM {{ polygon_garagem }}
), box AS
/*1. Filtra registros que estão fora de uma caixa que contém a área do município de Rio de Janeiro.*/
(
	SELECT  *
	FROM {{ limites_caixa }}
), gps AS
/*2. Filtra registros antigos. Remove registros que tem diferença maior que 1 minuto entre o timestamp_captura e timestamp_gps.*/
(
	SELECT  *
	       ,ST_GEOGPOINT(longitude,latitude) posicao_veiculo_geo
	FROM {{ registros }}
	WHERE DATETIME_DIFF(timestamp_captura, timestamp_gps, MINUTE) BETWEEN 0 AND 1
	AND data BETWEEN DATE({{ date_range_start }}) AND DATE({{ date_range_end }}) 
), filtrada AS
/*1,2,3 e 4. Muda o nome de variáveis para o padrão do projeto.*/
(
	SELECT  DISTINCT ordem AS id_veiculo
	       ,latitude
	       ,longitude
		   ,posicao_veiculo_geo
	       ,velocidade
	       ,linha
	       ,timestamp_gps
	       ,timestamp_captura
	       ,data
	       ,hora
	       ,extract(time FROM gps.timestamp_captura) AS hora_completa, 
		   ST_INTERSECTS(ponto, (SELECT  poly FROM garagem_polygon)) fora_garagem,
	FROM gps
	WHERE ST_INTERSECTSBOX(ponto, 
		(SELECT min_longitude FROM box), 
		(SELECT min_latitude FROM box), 
		(SELECT max_longitude FROM box), 
		(SELECT max_latitude FROM box))
)
SELECT  *
       ,STRUCT({{ maestro_sha }} AS versao_maestro,{{ maestro_bq_sha }} AS versao_maestro_bq) versao
FROM filtrada