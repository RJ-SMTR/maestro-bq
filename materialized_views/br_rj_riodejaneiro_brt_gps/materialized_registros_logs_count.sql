SELECT
  COUNT(sucesso) / {{ n_capturas_hora }} as porcentagem_sucesso,
  SAFE_CAST(DATETIME(TIMESTAMP(data), "America/Sao_Paulo") AS DATETIME) data,
  extract(hour from timestamp_captura) as hora,
  {{ run_timestamp }} AS run_timestamp,
  {{ maestro_sha }} AS maestro_sha,
  {{ maestro_bq_sha }} AS maestro_bq_sha
FROM `rj-smtr.br_rj_riodejaneiro_brt_gps.materialized_registros_logs`
WHERE timestamp_captura >= {{ date_range_start }} AND timestamp_captura <= {{ date_range_end }}
GROUP BY data, hora