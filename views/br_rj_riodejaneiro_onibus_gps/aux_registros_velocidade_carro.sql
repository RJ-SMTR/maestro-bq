/*

SQL Flavor: Standard SQL

Propósito:
  Calcula as velocidades médias de cada carro, identificado por 'placa', para os quais estejamos capturando posições válidas.
*/
/*
wrows:
  ST_GEOGPOINT é usado em 'latitude'/'longitude' por conveniência no cálculo das distâncias usando funções geográficas disponíveis ao BigQuery.
  Então criamos o campo 'point' com os valores de lat/long para cada linha. ROW_NUMBER() é usado para indexar cada linha dentro da agregação e permitir, 
  nos próximos passos, o cálculo das distâncias entre pontos sucessivos ordenados por 'timestamp_captura', esses indíces são salvos no campo 'n_rows'
*/
WITH wrows AS (
  SELECT ST_GEOGPOINT(longitude, latitude) point, timestamp_captura, timestamp_gps, latitude, longitude, placa,
        ROW_NUMBER() OVER (PARTITION BY placa ORDER BY timestamp_captura) n_row
  from `rj-smtr.br_rj_riodejaneiro_onibus_gps.registros_tratada_8_dias`),
/*
distances:
  Neste passo calculamos a diferença de tempo ('minutos') e a distância entre dois pontos capturados consecutivamente. Para isso, nos valemos da indexação
  do passo anterior e realizamos uma junção da tabela (t1) com ela mesma, porém com todas as colunas deslocadas por 1 com relação ao índice, condição
  explicitada na cláusula JOIN ... ON t1.n_row = t2.n_row - 1.
    Ex:
    t1
      point   timestamp_captura   timestamp_gps         latitude  longitude placa    n_row 
    1 (x1,y1) 2021-07-27T00:05:00 2021-07-27T00:04:50   x1         y1       AAA0000   1
    2 (x2,y2) 2021-07-27T00:06:00 2021-07-27T00:05:40   x2         y2       AAA0000   2
    3 (x3,y3) 2021-07-27T00:07:00 2021-07-27T00:06:47   x3         y3       AAA0000   3
    
    t2
      point   timestamp_captura   timestamp_gps         latitude  longitude placa    n_row 
    1 (x1,y1) 2021-07-27T00:05:00 2021-07-27T00:04:50   x1         y1       AAA0000   1
    2 (x2,y2) 2021-07-27T00:06:00 2021-07-27T00:05:40   x2         y2       AAA0000   2
    3 (x3,y3) 2021-07-27T00:07:00 2021-07-27T00:06:47   x3         y3       AAA0000   3

    distances
      ts1                   ts2                   latitude    longitude     placa   minutos   distancia  
    1 2021-07-27T00:05:00   null                  x1           y1           AA000   null      null
    2 2021-07-27T00:06:00   2021-07-27T00:05:00   x2           y2           AA0000  ts2-ts1   ST_DISTANCE((x2,y2) - (x1,y1))
    3 2021-07-27T00:07:00   2021-07-27T00:06:00   x3           y3           AA0000  ts2-ts1   ST_DISTANCE((x3,y3) - (x2,y2))
*/  

distances AS (
  SELECT
    t1.timestamp_captura ts1, 
    t2.timestamp_captura ts2, 
    t1.latitude, t1.longitude, t1.placa,
    DATETIME_DIFF(t2.timestamp_captura, t1.timestamp_captura, SECOND) / 60 minutos,
    ST_DISTANCE(t1.point, t2.point) distancia
  FROM wrows t1
  JOIN wrows t2
  ON t1.n_row = t2.n_row -1
  AND t1.placa = t2.placa
  ),
/*
times:
  Neste passo, criamos uma tabela vazia com as faixas de 10 minutos, as quais popularemos com as velocidades médias calculadas por faixa.
  Fazemos isso, selecionando a data mínima e máxima presente no dado base (`rj-smtr.br_rj_riodejaneiro_onibus_gps.registros_tratada_8_dias`) e utilizando
  GENERATE_TIMESTAMP_ARRAY para gerar todas as timestamps para os intervalos de 10 minutos compreendidos entre as datas máxima e mínima.
    Ex:
    times
      min_date              max_date              ts
    1 2021-07-27T00:00:00   2021-07-28T00:00:00   2021-07-27T00:10:00
    2 2021-07-27T00:00:00   2021-07-28T00:00:00   2021-07-27T00:20:00
    3 2021-07-27T00:00:00   2021-07-28T00:00:00   2021-07-27T00:30:00
    ...
*/
times AS (
  SELECT ts
  FROM (
    SELECT
        CAST(MIN(data) AS TIMESTAMP) min_date, TIMESTAMP_ADD(CAST(MAX(data) AS TIMESTAMP), INTERVAL 1 DAY) max_date
    FROM `rj-smtr.br_rj_riodejaneiro_onibus_gps.registros_tratada_8_dias`) t 
  JOIN UNNEST(GENERATE_TIMESTAMP_ARRAY(t.min_date, t.max_date, INTERVAL 10 MINUTE)) ts
),
/*
speed:
  Aqui, a tabela é uma junção simples dos timestamps gerados em times ('ts') com as colunas de distances. Esse é passo de popular a tabela vazia mencionado
  acima. Essa tabela terá os campos 'ts' (de times), 'ts1' e 'ts2' (de distances), assim para a junção usamos a seguinte condição:
    ts2 não ser menor que ts: como ts2 é o ponto futuro para cálculo da velocidade, não podemos tê-lo menor que o valor de 'ts' que inicia um intervalo de
    10 minutos.
    ts1 não pode ser maior que ts + 10 min: como ts1 é o ponto inicial para calculo da velocidade, essa condição impede que peguemos valores de ts1 na próxima
    faixa de 10 minutos.
  Quando satisfazemos as condições, teremos diversos pares 'ts1', 'ts2' associados ao mesmo 'ts', permitindo que agrupemos por 'ts' e calculemos as médias de
  velocidade com esse agrupamento.
*/
speed AS (
  SELECT
    ts,
    d.*
  FROM times
  JOIN distances d
  ON NOT(
      ts2 < DATETIME(ts) OR 
      ts1 > DATETIME_ADD(DATETIME(ts), INTERVAL 10 MINUTE))
 )
/*
Finalmente, podemos calcular as velocidades. Selecionamos 'ts2' como valor para 'timestamp_captura' pois só em 'ts2' não nulo teremos valores não nulos para
a 'distancia' e 'minutos' (em speed). Neste passo, fazemos a junção de speed com uma subconsulta à própria speed. A subconsulta na cláusula JOIN é usada para
calcular as velocidades médias (AVG(SAFE_DIVIDE(distancia,minutos)), o fator 6/100 é uma conversão de unidades para km/h) agregando por placa (para usar 
somente dados de carros individuais) e por 'ts', que define as faixas de 10 minutos nas quais a velocidade média será avaliada. 
No resultado final, calculamos novamente a média das velocidades dadas na subconsulta, porém agora agregado em 'ts2' e com junção nas faixas horárias('ts') e
nas placas.
*/
SELECT
  ts2 as timestamp_captura, t1.placa, latitude, longitude, AVG(t1.velocidade) velocidade
FROM speed
JOIN (SELECT ts, placa, avg(SAFE_DIVIDE(distancia, minutos) * 6/100) velocidade 
      FROM speed 
      GROUP BY ts, placa) t1
ON t1.ts = speed.ts 
AND t1.placa = speed.placa
GROUP BY ts2, placa, latitude, longitude