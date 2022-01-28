with gps as (
  SELECT
    *,
    ROUND(SAFE_DIVIDE(
        COUNT(CASE WHEN flag_em_movimento is true THEN 1 END) over(partition by id_veiculo, data, hora),
        COUNT(distinct timestamp_gps) over(partition by id_veiculo, data, hora)
      ),1) perc_operacao,
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
    ) g 
  WHERE data between DATE({{ date_range_start }}) and DATE({{ date_range_end}})
),
ap as (
  SELECT
    REPLACE(regiao_ap,".", "") regiao_ap,
    geometry as rp
  FROM {{ ap }}
),
detalhes as (
  SELECT
    g.*,
    CASE
      WHEN
        ST_DWITHIN(posicao_veiculo_geo, ap.rp, {{ tamanho_buffer_metros }})
      THEN
        true
    ELSE
      false
    END flag_ap_correta,
    CASE
      WHEN
        perc_operacao >  {{perc_operacao_minima }}
      THEN
          CASE
            WHEN
                lead(perc_operacao) over (partition by id_veiculo, data, hora order by hora) <  {{perc_operacao_minima }}
                OR
                hora = max(hora) over(partition by id_veiculo, data)
            THEN
                'inicio'
            WHEN
                lag(perc_operacao) over (partition by id_veiculo, data, hora order by hora) < {{perc_operacao_minima }}
                OR
                hora = min(hora) over(partition by id_veiculo, data)
            THEN
                'fim'
          ELSE 'multavel'
          END
    ELSE 'fora operacao' 
    END tipo_hora,
    CASE
      WHEN
        n_registros < {{ n_registros_minimo }}
      THEN
        false
    ELSE
      true
    END flag_transmitindo_regular
      
  FROM gps g
  LEFT JOIN ap 
  ON regiao_ap = g.rp
)
SELECT 
    d.*
FROM  detalhes d
