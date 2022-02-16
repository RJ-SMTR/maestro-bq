SELECT
  *
FROM (
  SELECT
    SAFE_CAST(data_versao AS DATE) data_versao,
    json_value(content,
      '$.IDFrotaDeterminada') IDFrotaDeterminada,
    json_value(content,
      '$.TipoOnibusID') TipoOnibusID,
    SAFE_CAST(json_value(content,
        '$.FrotaDeterminada') AS INT64) FrotaDeterminada,
    SAFE_CAST(json_value(content,
        '$.FrotaServico') AS INT64) FrotaServico,
    SAFE_CAST(json_value(content,
        '$.dataInicioVigencia') AS DATETIME) dataInicioVigencia,
    SAFE_CAST(json_value(content,
        '$.dataFimVigencia') AS DATETIME) dataFimVigencia,
    json_value(content,
      '$.legislacaoInicioVigencia') legislacaoInicioVigencia,
    json_value(content,
      '$.legislacaoFimVigencia') legislacaoFimVigencia,
    json_value(content,
      '$.route_id') route_id
  FROM
    `rj-smtr.br_rj_riodejaneiro_sigmob.frota_determinada`) f
WHERE
  (dataFimVigencia IS NULL
    OR DATE(dataFimVigencia) >= DATE(data_versao))
  AND (dataInicioVigencia IS NULL
    OR DATE(dataInicioVigencia) <= DATE(data_versao))
