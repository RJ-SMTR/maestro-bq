SELECT
  agg.table.*
FROM (
  SELECT
    nome_estacao,
    id_problema,
    ARRAY_AGG(STRUCT(table)
    ORDER BY
      dt DESC)[SAFE_OFFSET(0)] agg
    from {{ ref('questionario_melted_completa') }} table
  GROUP BY
    nome_estacao,
    id_problema
    )