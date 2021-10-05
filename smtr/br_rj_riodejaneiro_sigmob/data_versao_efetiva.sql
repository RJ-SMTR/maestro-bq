SELECT 
    data,
    data_versao as data_versao_original, 
    CASE WHEN data < '{{ data_inclusao_shapes }}' THEN '{{data_inclusao_shapes}}' ELSE
        LAST_VALUE(data_versao IGNORE NULLS) OVER (ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS data_versao_efetiva
FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', CURRENT_DATE())) data
LEFT JOIN (SELECT DISTINCT data_versao
    FROM rj-smtr-dev.br_rj_riodejaneiro_sigmob.shapes_geom)
ON data = data_versao