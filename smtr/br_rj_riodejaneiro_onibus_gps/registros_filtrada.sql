WITH garagem_polygon AS (
  SELECT 
    ST_GEOGFROMTEXT(WKT, make_valid => true) AS poly
  FROM {{ polygon_garagem }}
),
box AS (
  SELECT
    *
  FROM {{ limites_caixa }}
),
gps AS (
  SELECT 
    *,
    ST_GEOGPOINT(longitude, latitude) ponto
  FROM {{ registros }}
  WHERE DATETIME_DIFF(timestamp_captura, timestamp_gps, MINUTE) < 2
  and data between DATE({{ date_range_start }}) and DATE({{ date_range_end }})
),
filtrada as (
SELECT DISTINCT
  ordem as id_veiculo,
  latitude,
  longitude, 
  timestamp_gps, 
  velocidade, 
  linha, 
  timestamp_captura, 
  data, 
  hora,
  extract(time from gps.timestamp_captura) as hora_completa,
  ST_INTERSECTS(ponto, (SELECT poly FROM garagem_polygon)) fora_garagem,
FROM gps
WHERE ST_INTERSECTSBOX(ponto, (SELECT min_longitude FROM box), (SELECT min_latitude FROM box), (SELECT max_longitude FROM box), (SELECT max_latitude FROM box))
)
SELECT *,
      STRUCT({{ maestro_sha }} AS versao_maestro, {{ maestro_bq_sha }} AS versao_maestro_bq) versao
FROM filtrada

