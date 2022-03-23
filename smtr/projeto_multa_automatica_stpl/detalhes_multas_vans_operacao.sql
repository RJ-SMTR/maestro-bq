with rho as (
  SELECT 
    operadora,
    DATE(data_transacao) data_transacao,
    hora_transacao,
    linha,
    total_pagantes,
    timestamp_captura,
    min(hora_transacao) over(partition by operadora, data_transacao) primeira_hora,
    max(hora_transacao) over(partition by operadora, data_transacao) ultima_hora

  FROM {{ rho }} r 
  WHERE
  ano = extract(year from DATE_SUB(DATE({{ date_range_end }}), INTERVAL 14 DAY))
  AND mes = extract(month from DATE_SUB(DATE({{ date_range_end }}), INTERVAL 14 DAY))
  AND data_transacao = DATE_SUB(DATE({{ date_range_end }}), INTERVAL 14 DAY)
),
status_captura as (
  SELECT
    data,
    extract(hour from timestamp_captura) hora,
    COUNT(CASE WHEN sucesso is true THEN 1 END) < {{ n_minimo_sucessos_captura }} flag_falha_captura_smtr 
  FROM {{ registros_logs }} 
  WHERE data = DATE_SUB(DATE({{ date_range_end }}), INTERVAL 14 DAY)
  GROUP by data, hora
),
combinacoes as (
  SELECT
    operadora,
    data_transacao,
    hora,
    primeira_hora,
    ultima_hora
  FROM unnest(generate_array(0,23)) hora
  FULL OUTER JOIN (
    SELECT DISTINCT
    operadora,
    data_transacao,
    primeira_hora,
    ultima_hora
    FROM rho
  ) r
  ON 1=1
),
transacoes as (
  SELECT 
    c.*,
    coalesce(total_pagantes, 0) n_transacoes,
  FROM combinacoes c
  LEFT JOIN rho r
  ON c.operadora = r.operadora
  AND c.data_transacao = r.data_transacao 
  AND c.hora = r.hora_transacao
),
detalhes_agg as (
  SELECT
    data,
    hora,
    id_veiculo,
    operadora,
    d.servico,
    n_movimento,
    n_registros,
    ROUND(
    SAFE_DIVIDE(
      count(CASE WHEN flag_ap_correta is false THEN 1 END),
      n_registros),
    2) perc_area_incorreta,
  FROM {{ detalhes_veiculo }} d
  WHERE data = DATE_SUB(DATE({{ date_range_end }}), INTERVAL 14 DAY)
  group by 1,2,3,4,5,6,7
),
catraca as (
  SELECT 
    d.* except(operadora),
    t.* except(hora),
    t.hora hora_transacao,
    CASE
      WHEN 
        n_transacoes = 0
      THEN
        false
    ELSE
      true
    END flag_catracando,
    CASE
      WHEN
        d.hora = primeira_hora
        OR 
        d.hora = ultima_hora
      THEN
        'abono'
    ELSE
      'passivel'
    END tipo_hora
  FROM detalhes_agg d
  FULL OUTER JOIN transacoes t
  ON
    d.operadora = t.operadora
    AND d.data = t.data_transacao
    AND d.hora = t.hora
),
multas_catracando as (
  SELECT 
    *,
    CASE
      WHEN
        tipo_hora = 'passivel'
        AND
        perc_area_incorreta > {{ perc_area_incorreta_maxima }}
      THEN
        'local proibido'
      WHEN
        n_registros = 0
      THEN
        'gps desligado'
    END tipo_multa
    
  FROM catraca c
  WHERE 
    flag_catracando is true
),
multas_nao_catracando as (
  SELECT
    *,
    CASE
      WHEN
        perc_area_incorreta > 0
      THEN
        'percentil sobre local proibido'
    END tipo_multa
  FROM catraca c
  WHERE flag_catracando is false
)
SELECT m.* except(rn)
FROM (
SELECT 
  *,
  row_number() over(partition by id_veiculo, data, tipo_multa order by hora) rn
from multas_catracando where tipo_multa is not null
UNION ALL
select 
  *,
  row_number() over(partition by id_veiculo, data, tipo_multa order by hora) rn
from multas_nao_catracando where tipo_multa is not null
) m
JOIN (
  SELECT *
  FROM status_captura
  WHERE flag_falha_captura_smtr is false
) s
ON m.data = s.data
AND m.hora = s.hora
WHERE rn = 1
