WITH
consorcios AS (
  SELECT
    linha,
    codigo AS permissao,
  FROM `rj-smtr-dev.br_rj_riodejaneiro_sigmob.frota_determinada_consorcio` 
),
sumario AS (
  SELECT 
    id_multa,
    linha,
    artigo_multa as codigo_infracao,
    concat(
      replace(data, "-", ""),
      replace(faixa_horaria, ":", "")
    ) as data_infracao
  FROM `rj-smtr-dev.projeto_multa_automatica.sumario_multa_linha_onibus` 
  WHERE DATE(data) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
)

SELECT
  permissao,
  s.*
FROM sumario s
JOIN consorcios c
ON s.linha=c.linha