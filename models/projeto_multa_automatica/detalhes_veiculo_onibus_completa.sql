/*
 Descrição: Puxa os dados de GPS do SPPO por timestamp, cria a faixa
 horária (10 minutos) e coluna de situação da operação (em
 operação/garagem) 
*/

select distinct
    data,
    servico as linha,
    id_veiculo,
    extract(time from timestamp_seconds(60 * div(unix_seconds(timestamp(timestamp_gps)), 60))) timestamp_minuto,
    cast(extract(time from timestamp_seconds(
        {{ var('faixa_horaria') * 60 }} * div(
                                    unix_seconds(timestamp(timestamp_gps)), 
                                    {{ var('faixa_horaria') * 60 }}))) as string) faixa_horaria,
    case
        when tipo_parada = 'garagem' then 'garagem'
        else 'operando'
    end situacao
from {{ source('br_rj_riodejaneiro_veiculos', 'gps_sppo') }}
