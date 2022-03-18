with gps as (
  SELECT
    g.*,
    p.operadora,
    COUNT(CASE WHEN flag_em_movimento is true THEN 1 END) over(partition by id_veiculo, data, hora) n_movimento,
    COUNT(distinct timestamp_gps) over(partition by id_veiculo, data, hora) n_registros
  FROM (
    SELECT 
      data,
      extract(hour from timestamp_gps) hora,
      id_veiculo,
      timestamp_gps,
      servico,
      SUBSTR(servico, 5, 2) rp,
      ST_GEOGPOINT(longitude, latitude) posicao_veiculo_geo,
      flag_em_movimento
    FROM {{ gps_stpl }}
    WHERE data between DATE({{ date_range_start }}) and DATE({{ date_range_end }})
    ) g
  JOIN {{ aux_stpl_permissionario }} p
  ON g.id_veiculo = p.identificador
  AND p.data_versao = g.data
  WHERE codigo_bloqueio is null
),
ap as (
  SELECT
    CASE
      WHEN
        regiao_ap = '4'
      THEN '41'
    ELSE
      REPLACE(regiao_ap,".", "")
    END regiao_ap,
    geometry as rp_geom
  FROM {{ ap }}
),
detalhes as (
  SELECT
    g.*,
    CASE
      WHEN
        ST_INTERSECTS(posicao_veiculo_geo, rp_geom)
      THEN
        true
    ELSE
      false
    END flag_ap_correta,
  FROM gps g
  LEFT JOIN ap 
  ON regiao_ap = g.rp
)
SELECT 
    d.*
FROM  detalhes d

