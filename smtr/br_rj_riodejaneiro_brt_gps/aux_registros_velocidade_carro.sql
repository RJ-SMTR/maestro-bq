WITH 
  registros AS (
    SELECT   
      ST_GEOGPOINT(longitude, latitude) point,
      lag(ST_GEOGPOINT(longitude, latitude)) over (partition by placa_veiculo order by timestamp_gps) prev_point,
      lag(timestamp_gps) over (partition by placa_veiculo, id_veiculo order by timestamp_gps) ts1,
      timestamp_gps ts2,
      timestamp_captura,
      latitude,
      longitude,
      id_veiculo,
      placa_veiculo,
      linha,
      data,
    from {{ registros_filtrada }}   
  ),  
  distances AS (
    SELECT
        ts1, 
        ts2, 
        latitude,
        longitude, 
        id_veiculo,
        placa_veiculo,
        timestamp_captura,
        linha,
        DATETIME_DIFF(ts2, ts1, SECOND) / 60 minutos,
        ST_DISTANCE(prev_point, point) distancia,
        data
    FROM registros
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
  ts2 as timestamp_gps,
  timestamp_captura, 
  data, 
  t1.placa_veiculo,
  id_veiculo, 
  linha, 
  latitude, 
  longitude, 
  round(AVG(t1.velocidade), 1) velocidade,
  STRUCT({{ maestro_sha }} AS versao_maestro, {{ maestro_bq_sha }} AS versao_maestro_bq) versao
FROM speed
JOIN (SELECT ts, placa_veiculo, avg(SAFE_DIVIDE(distancia, minutos) * 6/100) velocidade 
      FROM speed 
      GROUP BY ts, placa_veiculo) t1
ON t1.ts = speed.ts 
AND t1.placa_veiculo = speed.placa_veiculo
GROUP BY ts2, timestamp_captura, data, id_veiculo, placa_veiculo, linha, latitude, longitude