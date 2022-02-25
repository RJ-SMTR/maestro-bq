
with rho as (
  SELECT 
      operadora,
      DATE(data_transacao) data_transacao,
      hora_transacao,
      total_pagantes + total_gratuidades n_transacoes
  FROM `rj-smtr.br_rj_riodejaneiro_rdo.rho5_registros_stpl`
  WHERE ano = 2021
  AND mes = 12
  AND dia between 10 and 17
  and data_transacao between '2021-12-10' and '2021-12-17'
  --where timestamp_captura = (select max(timestamp_captura) from rj-smtr.br_rj_riodejaneiro_rdo.rho5_registros_stpl)
),
combinacoes as (
  SELECT 
    p.operadora,
    p.codigo_hash as id_veiculo,
    data,
    hora
  FROM (select min(data_transacao) min_data, max(data_transacao) max_data from rho),
  UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(min_data, INTERVAL 1 DAY), max_data, INTERVAL 1 DAY)) data,
  UNNEST(GENERATE_ARRAY(0,23)) hora
  CROSS JOIN (select * from rj-smtr-dev.br_rj_riodejaneiro_veiculos.aux_stpl_permissionario where operadora != '') p
),
gps as (
  SELECT 
      c.id_veiculo,
      c.data,
      c.hora,
      c.operadora,
      COALESCE(n_registros,0) n_registros
  FROM combinacoes c
  LEFT JOIN (
    SELECT
      data,
      extract(hour from timestamp_gps) hora,
      id_veiculo,
      servico,
      COUNT(distinct timestamp_gps) n_registros
    FROM `rj-smtr-dev.br_rj_riodejaneiro_veiculos.gps_stpl`
    WHERE data between (select min(data) from combinacoes) and (select max(data) from combinacoes) 
    GROUP BY 1,2,3,4) d
    ON
      c.id_veiculo = d.id_veiculo
      AND c.data = d.data
      AND c.hora = d.hora
),
transacoes as (
  SELECT
    g.*,
    COALESCE(n_transacoes,0) n_transacoes
    
  FROM gps g
  LEFT JOIN rho r
  ON
    g.operadora = r.operadora
    AND g.data = r.data_transacao
    AND g.hora = r.hora_transacao 
),
veiculos_nao_operantes as (
  SELECT
    t.*,
    SUM(n_transacoes) over (partition by operadora, id_veiculo, data order by hora range between 11 preceding and current row) sum_transac,
    SUM(n_registros) over (partition by operadora, id_veiculo, data order by hora range between 11 preceding and current row) sum_regs,
    CASE
      WHEN
        ARRAY_LENGTH(ARRAY_AGG(n_registros) over (partition by operadora, data order by hora range between 11 preceding and current row)) = 12
        AND
        SUM(n_transacoes) over (partition by operadora, data order by hora range between 11 preceding and current row) = 0
        AND
        SUM(n_registros) over (partition by operadora, data order by hora range between 11 preceding and current row) = 0
      THEN
        "não operação"
    END tipo_multa
  FROM transacoes t
)
SELECT *
FROM veiculos_nao_operantes 
where tipo_multa is not null
order by operadora, id_veiculo, data, hora

