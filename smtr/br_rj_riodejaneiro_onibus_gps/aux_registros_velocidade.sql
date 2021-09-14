/*
Descrição:
Estimativa das velocidades dos veículos nos últimos 10 minutos contados a partir da timestamp_gps atual.
Essa metodologia serve para determinar quais carros estão em movimento e quais estão parados.
1. Calculamos a velocidade do veículo no último trecho de 10 minutos de operação. 
A implementação utiliza a função 'first_value' com uma janela (cláusula 'over') de até 10 minutos anteriores à 
timestamp_gps atual e calcula a distância do ponto mais antigo (o first_value na janela) ao ponto atual (posicao_veiculo_geo).
Dividimos essa distância pela diferença de tempo entre a timestamp_gps atual e a timestamp_gps do ponto mais
antigo da janela (o qual recuperamos novamente com o uso de first_value).
Esta diferença de tempo (datetime_diff) é calculada em segundos, portanto multiplicamos o resultado da divisão por um fator
3.6 para que a velocidade esteja em quilômetros por hora. O resultado final é arrendondado sem casas decimais.
Por fim, cobrimos esse cálculo com a função 'if_null' e retornamos zero para a velocidade em casos onde a divisão retornaria
um valor nulo.
2. Após o calculo da velocidade, definimos a coluna 'status_movimento'. Veículos abaixo da 'velocidade_limiar_parado', são
considerados como 'parado'. Caso contrário, são considerados 'andando'
*/
with v as (
    select distinct 
        data,
        id_veiculo,
        timestamp_gps,
        -- 1. Cálculo da velocidade. 
        ifnull(round(safe_divide(
            st_distance(
                first_value(posicao_veiculo_geo) over (partition by id_veiculo 
                    order by unix_seconds(timestamp(timestamp_gps)) asc
                    range between 600 preceding and current row),
                posicao_veiculo_geo
                ),
            datetime_diff(
                timestamp_gps,
                first_value(timestamp_gps) over (partition by id_veiculo 
                    order by unix_seconds(timestamp(timestamp_gps)) asc
                    range between 600 preceding and current row), 
                SECOND
                )
            ) * 3.6, 0), 0) as velocidade
    from  {{ registros_filtrada }})
SELECT
  timestamp_gps, 
  data,
  id_veiculo, 
  velocidade,
  -- 2. Determinação do estado de movimento do veículo.
  case
    when velocidade < {{ velocidade_limiar_parado }} then 'parado'
    else 'andando'
  end status_movimento,
  STRUCT({{ maestro_sha }} AS versao_maestro, {{ maestro_bq_sha }} AS versao_maestro_bq) versao
FROM v