with gps as (
  SELECT
    *,
    CASE
      WHEN
        hora = min(hora) over (partition by data, id_veiculo)
        OR
        hora = max(hora) over(partition by data, id_veiculo)
      THEN
        false
    ELSE
      true
    END hora_valida,
  FROM (
    SELECT 
      data,
      extract(hour from timestamp_gps) hora,
      id_veiculo,
      timestamp_gps,
      servico,
      SUBSTR(servico, 5, 2) rp,
      ST_GEOGPOINT(longitude, latitude) posicao_veiculo_geo,
      extract(time from timestamp_seconds(
        {{ faixa_horaria * 60 }}* div(
        unix_seconds(timestamp(timestamp_gps)), {{ faixa_horaria *60 }}))) faixa_horaria,
      flag_em_movimento
    FROM {{ gps_stpl }}
    ) g 
  WHERE data between DATE({{ date_range_start }}) and DATE({{ date_range_end }})
  AND timestamp_gps > {{ date_range_start }} and timestamp_gps <= {{ date_range_end }}
),
ap as (
  SELECT
    REPLACE(regiao_ap,".", "") regiao_ap,
    geometry as rp
  FROM {{ ap }}
)
SELECT 
    g.*,
    CASE
      WHEN
        ST_DWITHIN(posicao_veiculo_geo, ap.rp, {{ tamanho_buffer_metros }})
      THEN
        true
    ELSE
      false
    END flag_ap_correta
FROM gps g
LEFT JOIN ap 
ON regiao_ap = g.rp
WHERE hora_valida is true