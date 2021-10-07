WITH
consorcios as (
  SELECT 
    l.consorcio,
    codigo as permissao,
    linha,  
  FROM (
    select * 
    from {{ linhas }}
    WHERE servico = 'REGULAR') l
  join (
    select
      codigo,
      consorcio
    from {{ codigos_consorcios }} 
  ) c
  on Normalize_and_Casefold(l.consorcio) = Normalize_and_Casefold(c.consorcio)
),
sumario AS (
  SELECT
    linha,
    artigo_multa as codigo_infracao,
    concat(
      replace(cast(data as string), "-", ""),
      replace(faixa_horaria, ":", "")
    ) as data_infracao
  FROM {{ sumario_multa_linha_onibus }}
  WHERE DATE(data) = CURRENT_DATE()
)

SELECT
  permissao,
  s.*
FROM sumario s
JOIN consorcios c
ON s.linha=c.linha