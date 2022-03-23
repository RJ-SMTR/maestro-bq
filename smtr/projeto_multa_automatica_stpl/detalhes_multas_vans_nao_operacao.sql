
with rho as (
  SELECT 
      operadora,
      DATE(data_transacao) data_transacao,
      hora_transacao,
      total_pagantes + total_gratuidades n_transacoes
  FROM {{ rho }}
  WHERE
  ano = extract(year from DATE_SUB(DATE({{ date_range_end }}), INTERVAL 14 DAY))
  AND mes = extract(month from DATE_SUB(DATE({{ date_range_end }}), INTERVAL 14 DAY))
  AND data_transacao = DATE_SUB(DATE({{ date_range_end }}), INTERVAL 14 DAY)
  --where timestamp_captura = (select max(timestamp_captura) from rj-smtr.br_rj_riodejaneiro_rdo.rho5_registros_stpl)
),
combinacoes as (
  SELECT 
    p.operadora,
    p.identificador as id_veiculo,
    DATE_SUB(DATE({{date_range_end}}), INTERVAL 14 DAY) data,
    hora
  FROM (select * from {{  aux_stpl_permissionario }} where operadora != '' and data_versao = DATE_SUB(DATE({{ date_range_end }}), INTERVAL 14 DAY)) p,
  UNNEST(GENERATE_ARRAY(0,23)) hora
  
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
    FROM {{ gps_stpl }}
    WHERE data = DATE_SUB(DATE({{ date_range_end }}), INTERVAL 14 DAY) 
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
SELECT * except(rn)
FROM (
SELECT *, row_number() over(partition by id_veiculo, data, tipo_multa order by hora) rn
from veiculos_nao_operantes 
)
where tipo_multa is not null and rn = 1

