SELECT 
  SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) timestamp_captura,
  SAFE_CAST(sucesso AS BOOLEAN) sucesso,
  SAFE_CAST(erro AS STRING) erro,
  SAFE_CAST(data AS DATE) data,
  {{ run_timestamp }} AS run_timestamp,
  {{ maestro_sha }} AS maestro_sha,
  {{ maestro_bq_sha }} AS maestro_bq_sha
FROM `rj-smtr-staging.br_rj_riodejaneiro_brt_gps_staging.registros_logs`
WHERE timestamp_captura >= {{ date_range_start }} AND timestamp_captura <= {{ date_range_end }}