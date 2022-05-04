with operacao as (
    SELECT
        *
    FROM {{ detalhes_multa_operacao }}
    WHERE data = DATE_SUB(DATE({{date_range_end}}), INTERVAL 14 DAY)
),
nao_operacao as (
    SELECT
        *
    FROM {{ detalhes_multa_nao_operacao }}
    WHERE data = DATE_SUB(DATE({{date_range_end}}), INTERVAL 14 DAY)
)
SELECT 
    *
FROM (
    SELECT * FROM operacao
    UNION ALL
    SELECT * from nao_operacao
)
