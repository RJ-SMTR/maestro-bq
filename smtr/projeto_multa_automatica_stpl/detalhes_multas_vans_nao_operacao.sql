with rho as (
  SELECT 
      operadora,
      DATE(data_transacao) data_transacao,
      hora_transacao
    FROM {{rho }}
  where timestamp_captura = (select max(timestamp_captura) from {{ rho }})
),
combinacoes as (
  SELECT 
    operadora,
    codigo_hash as id_veiculo,
    data
  FROM (select min(data_transacao) min_data, max(data_transacao) max_data from rho),
  UNNEST(GENERATE_DATE_ARRAY(min_data, max_data, INTERVAL 1 DAY)) data
  CROSS JOIN {{ aux_stpl_permissionario }}
),
gps as (
  SELECT 
      c.id_veiculo,
      c.data,
      operadora,
      timestamp_gps
  FROM {{ gps_stpl }} s
  RIGHT JOIN combinacoes c
  ON
    c.id_veiculo = s.id_veiculo 
    AND c.data = s.data
),
status_operacao as (
  SELECT
    g.id_veiculo,
    g.operadora,
    g.data,
    COUNT(distinct timestamp_gps) n_registros,
    COUNT(distinct hora_transacao) n_transacoes
  FROM gps g
  LEFT JOIN rho r
  ON 
    r.operadora = g.operadora
    AND r.data_transacao = g.data
  GROUP BY id_veiculo, g.operadora, data
),
veiculos_nao_operantes as (
  SELECT
    id_veiculo,
    operadora,
    data,
    n_registros,
    n_transacoes,
    CASE
      WHEN
        n_registros = 0
        and
        n_transacoes = 0
      THEN
        'não operacao'
      END tipo_multa
  FROM status_operacao 
)
SELECT *
FROM veiculos_nao_operantes 
where tipo_multa is not null




