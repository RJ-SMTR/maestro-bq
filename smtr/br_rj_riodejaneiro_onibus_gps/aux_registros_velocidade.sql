-- Calcula velocidades nos ultimos 10 min
with v as (
  select distinct 
      data,
      id_veiculo,
      timestamp_gps, 
      ifnull(
          round(
              safe_divide(st_distance(
                  first_value(posicao_veiculo_geo) over 
                      (partition by id_veiculo 
                      order by unix_seconds(timestamp(timestamp_gps)) asc
                      range between 600 preceding and current row
                      ),
                  posicao_veiculo_geo),
              datetime_diff(timestamp_gps, first_value(timestamp_gps) over 
                  (partition by id_veiculo 
                  order by unix_seconds(timestamp(timestamp_gps)) asc
                  range between 600 preceding and current row
                  ), SECOND)
              ) * 3.6, 0), 0) velocidade
  from  {{ registros_filtrada }})
SELECT
  timestamp_gps, 
  data,
  id_veiculo, 
  velocidade,
  case
    when velocidade < {{ velocidade_limiar_parado }} then 'parado'
    else 'andando'
  end status_movimento,
  STRUCT({{ maestro_sha }} AS versao_maestro, {{ maestro_bq_sha }} AS versao_maestro_bq) versao
FROM v