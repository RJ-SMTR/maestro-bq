/*
Descrição:
Calcula se o veículo está dentro do trajeto correto dado o trajeto cadastrado no SIGMOB e 
a linha que está no registro.
1. Calcula as intersecções definindo um 'buffer', utilizado por st_dwithin para identificar se o ponto está à uma
distância menor ou igual ao buffer do traçado definido no SIGMOB.
2. Calcula um histórico de intersecções nos ultimos 10 minutos de registros de cada carro. Definimos que o carro é
considerado fora do trajeto definido se a cada 10 minutos, ele não esteve dentro do traçado planejado pelo menos uma
vez.
3. Identifica se a linha informada no registro capturado existe nas definições presentes no SIGMOB.
4. Como não conseguimos identificar o itinerário que o carro está realizando, no passo counts, os resultados de
intersecções são dobrados, devido ao fato de cada linha apresentar dois itinerários possíveis (ida/volta). Portanto,
ao final, realizamos uma agregação LOGICAL_OR que é true caso o carro esteja dentro do traçado de algum dos itinerários
possíveis para a linha informada.
*/
WITH
  registros AS (
    SELECT id_veiculo, linha, latitude, longitude, data, posicao_veiculo_geo, timestamp_gps
    FROM
    {{ registros_filtrada }} r 
  ),
counts AS (
  SELECT
    r.*,
    s.data_versao,
    s.linha_gtfs,
    CASE
      WHEN st_dwithin(shape, posicao_veiculo_geo, {{ tamanho_buffer_metros }}) THEN TRUE
      ELSE FALSE
    END AS flag_trajeto_correto,
    CASE
      WHEN 
        COUNT(CASE WHEN st_dwithin(shape, posicao_veiculo_geo, {{ tamanho_buffer_metros }}) THEN 1 END) 
        OVER (PARTITION BY id_veiculo 
              ORDER BY UNIX_SECONDS(TIMESTAMP(timestamp_gps)) 
              RANGE BETWEEN {{ intervalo_max_desvio_segundos }} PRECEDING AND CURRENT ROW) >= 1 
        THEN True
      ELSE False
    END AS flag_trajeto_correto_hist,
    CASE WHEN s.linha_gtfs IS NULL THEN False ELSE True END AS flag_linha_existe_sigmob 
  FROM (
    SELECT t1.*, t2.data_versao_efetiva
    FROM registros t1
    JOIN  {{ data_versao_efetiva }} t2
    ON t1.data = t2.data
  ) r
  LEFT JOIN (
    SELECT * 
    FROM {{ shapes }} 
    WHERE id_modal_smtr in ({{ id_modal_smtr|join(', ') }})
  ) s
  ON
    r.linha = s.linha_gtfs
  AND
    r.data_versao_efetiva = s.data_versao
)
SELECT
  id_veiculo,
  linha,
  linha_gtfs,
  data,
  timestamp_gps,
  LOGICAL_OR(flag_trajeto_correto) AS flag_trajeto_correto,
  LOGICAL_OR(flag_trajeto_correto_hist) AS flag_trajeto_correto_hist,
  LOGICAL_OR(flag_linha_existe_sigmob) AS flag_linha_existe_sigmob,
  STRUCT({{ maestro_sha }} AS versao_maestro, 
        {{ maestro_bq_sha }} AS versao_maestro_bq,
        data_versao AS data_versao_sigmob
        ) versao
FROM
  counts c
GROUP BY
  id_veiculo,
  linha,
  linha_gtfs,
  data,
  data_versao,
  timestamp_gps