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
        {{ faixa_horaria * 60 }} * div(
                                    unix_seconds(timestamp(timestamp_gps)), 
                                    {{ faixa_horaria * 60 }}))) as string) faixa_horaria,
    case
        when tipo_parada = 'garagem' then 'garagem'
        else 'operando'
    end situacao
from {{ gps_sppo }}
WHERE data BETWEEN DATE({{ date_range_start }}) AND DATE({{ date_range_end }})
and timestamp_gps > {{ date_range_start }} 
and timestamp_gps <= {{ date_range_end }};