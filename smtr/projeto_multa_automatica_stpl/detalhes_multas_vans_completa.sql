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
  ano = extract(year from {{ date_range_start }})
  AND mes between extract(month from {{ date_range_start }}) and extract(month from {{ date_range_end }})
  AND data_transacao between DATE({{ date_range_start }}) and DATE({{ date_range_end }})
),
detalhes as (
  select 
    d.*, operadora
  from {{ detalhes_veiculo }} d
  JOIN {{ aux_stpl_permissionario }} p
  ON p.codigo_hash = d.id_veiculo
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
    CASE
      WHEN
        hora_transacao is not null
      THEN
        true
    ELSE
      false
    END flag_catracando
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
        true not in unnest(
            array_agg(flag_ap_correta) over (
            partition by data, id_veiculo
            order by unix_seconds(TIMESTAMP(timestamp_gps,'America/Sao_Paulo'))
            range between {{ intervalo_nao_conformidade_minutos * 60 }} preceding and current row
              )
            )
      THEN
        'local proibido'
      WHEN
        tipo_hora != 'fora operacao'
        AND
        timestamp_gps is null
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
        true not in unnest(
            array_agg(flag_ap_correta) over (
            partition by data, id_veiculo
            order by unix_seconds(TIMESTAMP(timestamp_gps,'America/Sao_Paulo'))
            range between {{ intervalo_nao_conformidade_minutos * 60 }} preceding and current row
              )
            )
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




