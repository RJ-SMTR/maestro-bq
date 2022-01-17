WITH
    registros as (
    -- 1. registros_filtrada
    SELECT 
        id_veiculo,
        timestamp_gps,
        timestamp_captura,
        velocidade,
        linha,
        latitude,
        longitude,
        data,
        hora
    FROM {{ stpl_registros_filtrada }}
    WHERE data BETWEEN DATE({{ date_range_start }}) AND DATE({{ date_range_end }})
    AND timestamp_gps > {{ date_range_start }} and timestamp_gps <= {{ date_range_end }}
    ),
    velocidades AS (
    -- 2. velocidades
    SELECT
        id_veiculo, timestamp_gps, linha, velocidade, distancia, flag_em_movimento
    FROM {{ stpl_velocidade }} 
    WHERE data BETWEEN DATE({{ date_range_start }}) AND DATE({{ date_range_end }})
    AND timestamp_gps > {{ date_range_start }} and timestamp_gps <= {{ date_range_end }}
    ),
    flags AS (
    -- 4. flag_trajeto_correto
    SELECT
        id_veiculo,
        timestamp_gps, 
        linha,
        flag_linha_existe_sigmob,
        flag_trajeto_correto, 
        flag_trajeto_correto_hist
    FROM {{ stpl_flag_trajeto_correto }}
    WHERE data BETWEEN DATE({{ date_range_start }}) AND DATE({{ date_range_end }})
    AND timestamp_gps > {{ date_range_start }} and timestamp_gps <= {{ date_range_end }}
    )
-- 5. Junção final
SELECT
    "STPL" modo,
    r.timestamp_gps,
    r.data,
    r.id_veiculo,
    REPLACE(r.linha, "L", "STPL") servico,
    r.latitude,
    r.longitude,
    CASE 
        WHEN 
        flag_em_movimento IS true AND flag_trajeto_correto_hist is true
        THEN true
    ELSE false
    END flag_em_operacao,
    v.flag_em_movimento,
    flag_linha_existe_sigmob,
    flag_trajeto_correto,
    flag_trajeto_correto_hist,
    r.velocidade velocidade_instantanea,
    v.velocidade velocidade_estimada_10_min,
    v.distancia,
    STRUCT({{ maestro_sha }} AS versao_maestro, {{ maestro_bq_sha }} AS versao_maestro_bq) versao
FROM
    registros r

JOIN
    flags f
ON
    r.id_veiculo = f.id_veiculo
    AND r.timestamp_gps = f.timestamp_gps
    AND r.linha = f.linha

JOIN
    velocidades v
ON
    r.id_veiculo = v.id_veiculo
    AND  r.timestamp_gps = v.timestamp_gps
    AND  r.linha = v.linha