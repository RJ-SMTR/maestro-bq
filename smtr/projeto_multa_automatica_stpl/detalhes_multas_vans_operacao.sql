with rho as (
  select 
    operadora,
    DATE(data_transacao) data_transacao,
    hora_transacao,
    r.linha,
    total_pagantes,
    timestamp_captura,
  from {{ rho }} r 
  where
  ano between extract(year from DATE({{ date_range_start }})) and extract(year from DATE({{ date_range_start }})) 
  AND mes between extract(month from DATE({{ date_range_start }})) and extract(month from DATE({{ date_range_end }}))
  AND data_transacao between DATE({{ date_range_start }}) and DATE({{ date_range_end }})
),
detalhes as (
  select
    data,
    id_veiculo,
    operadora,
    d.servico,
    timestamp_gps,
    hora,
    perc_operacao,
    flag_ap_correta,
    n_registros,
    tipo_hora,
  from {{ detalhes_veiculo }} d
  JOIN {{ aux_stpl_permissionario }} p
  ON p.codigo_hash = d.id_veiculo
),
status_captura as (
-- decidir se haverá multa por falha na API (sucesso = false)
  SELECT 
    data,
    extract(hour from timestamp_captura) hora,
    CASE 
      WHEN
        COUNT(distinct timestamp_captura) < {{ n_minimo_sucessos_captura }}
      THEN
        true
    ELSE
      false
    END flag_falha_captura_smtr
  FROM {{ registros_logs }} 
  WHERE data between DATE({{ date_range_start }}) and DATE({{ date_range_end }})
  GROUP by data, hora
),
catraca as (
  SELECT
    d.id_veiculo,
    r.operadora,
    d.timestamp_gps,
    flag_ap_correta,
    perc_operacao,
    data,
    hora,
    data_transacao,
    hora_transacao,
    tipo_hora,
    n_registros,
    SAFE_DIVIDE(
    COUNT(CASE WHEN flag_ap_correta is true then 1 else 0 end) over(partition by d.id_veiculo, data, hora),
    COUNT(distinct timestamp_gps) over(partition by d.id_veiculo, data, hora)
    ) perc_area_correta,
    CASE
      WHEN
        hora_transacao is not null
      THEN
        true
    ELSE
      false
    END flag_catracando,
  FROM rho r
  FULL OUTER JOIN detalhes d
  ON r.operadora = d.operadora
  AND d.data = r.data_transacao
  AND d.hora = r.hora_transacao 
),
multa_catracando as (
  SELECT
    *,
    CASE
      WHEN
        tipo_hora = 'multavel'
        AND
        perc_area_correta < 0.5
      THEN
        'local proibido'
      WHEN
        tipo_hora != 'fora operacao'
        AND
        n_registros < 20
      THEN
        'gps desligado'
    ELSE
      null
    END as tipo_multa,
  FROM catraca c
  WHERE 
  flag_catracando is true
),
multa_nao_catracando as (
   SELECT 
    *,
    CASE 
      WHEN
        perc_area_correta < {{ perc_area_correta_minima }}
      THEN
        'percentil local proibido'
    ELSE
      null
    END as tipo_multa,
   FROM catraca c
   where flag_catracando is false
   and tipo_hora != 'fora operacao'
)
SELECT *
FROM (
SELECT * 
FROM multa_catracando m
WHERE tipo_multa is not null
UNION ALL
SELECT * 
FROM multa_nao_catracando n
WHERE tipo_multa is not null
)



