-- Calcula velocidades nos ultimos 10 min
WITH wrows AS (
  SELECT ST_GEOGPOINT(longitude, latitude) point, timestamp_captura, timestamp_gps, latitude, longitude, id_veiculo, linha, data,
        ROW_NUMBER() OVER (PARTITION BY id_veiculo ORDER BY timestamp_captura) n_row
  from {{ registros_filtrada }}
  where data between DATE({{ date_range_start }}) and DATE({{ date_range_end }})   
),  
distances AS (
  SELECT
    t1.timestamp_captura ts1, 
    t2.timestamp_captura ts2, 
    t1.latitude, t1.longitude, t1.id_veiculo, t1.linha,
    DATETIME_DIFF(t2.timestamp_captura, t1.timestamp_captura, SECOND) / 60 minutos,
    ST_DISTANCE(t1.point, t2.point) distancia,
    t1.data
  FROM wrows t1
  JOIN wrows t2
  ON t1.n_row = t2.n_row -1
  AND t1.id_veiculo = t2.id_veiculo
  ),
times AS (
  SELECT ts
  FROM (
    SELECT
        CAST(MIN(data) AS TIMESTAMP) min_date, TIMESTAMP_ADD(CAST(MAX(data) AS TIMESTAMP), INTERVAL 1 DAY) max_date
  FROM {{ registros_filtrada }} 
  WHERE data BETWEEN DATE({{ date_range_start }}) and DATE({{ date_range_end }})) t 
  JOIN UNNEST(GENERATE_TIMESTAMP_ARRAY(t.min_date, t.max_date, INTERVAL {{ faixa_horaria_minutos }} MINUTE)) ts
),
speed AS (
  SELECT
    ts,
    d.*
  FROM times
  JOIN distances d
  ON NOT(
      ts2 < DATETIME(ts) OR 
      ts1 > DATETIME_ADD(DATETIME(ts), INTERVAL {{ faixa_horaria_minutos }} MINUTE))
 )
SELECT
  ts2 as timestamp_captura, data, t1.id_veiculo, linha, latitude, longitude, AVG(t1.velocidade) velocidade,
  STRUCT({{ maestro_sha }} AS versao_maestro, {{ maestro_bq_sha }} AS versao_maestro_bq) versao
FROM speed
JOIN (SELECT ts, id_veiculo, avg(SAFE_DIVIDE(distancia, minutos) * 6/100) velocidade 
      FROM speed 
      GROUP BY ts, id_veiculo) t1
ON t1.ts = speed.ts 
AND t1.id_veiculo = speed.id_veiculo
GROUP BY ts2, data, id_veiculo, linha, latitude, longitude