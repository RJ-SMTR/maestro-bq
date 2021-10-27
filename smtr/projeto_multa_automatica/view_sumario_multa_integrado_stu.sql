WITH
consorcios as (
  SELECT 
    r.consorcio,
    codigo as permissao,
    linha,
    data_versao  
  FROM (
    select 
      agency_name consorcio,
      route_short_name linha,
      data_versao
    from {{ routes }}) r
  join (
    select
      codigo,
      consorcio
    from {{ codigos_consorcios }} 
  ) c
  on Normalize_and_Casefold(r.consorcio) = Normalize_and_Casefold(c.consorcio)
),
sumario AS (
  SELECT
    "" as placa,
    "" as ordem,
    linha,
    artigo_multa as codigo_infracao,
    TIMESTAMP(concat(cast(s.data as string), " ", cast(faixa_horaria as string))) as data_infracao,
    s.data,
    data_versao_efetiva
  FROM {{ sumario_multa_linha_onibus }} s
  JOIN (
    select 
      data, 
      data_versao_efetiva_routes data_versao_efetiva
    from {{ data_versao_efetiva }}
  ) d
  on s.data = d.data
  where prioridade = 1
)

SELECT
  permissao,
  placa,
  ordem,
  s.linha,
  codigo_infracao,
  data_infracao,
  s.data
FROM sumario s
JOIN consorcios c
ON s.linha=c.linha
and s.data_versao_efetiva = c.data_versao
