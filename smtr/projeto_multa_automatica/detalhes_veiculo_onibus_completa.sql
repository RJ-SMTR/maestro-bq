select distinct
    data,
    linha,
    id_veiculo,
    extract(time from timestamp_seconds(60 * div(unix_seconds(timestamp(timestamp_gps)), 60))) timestamp_minuto,
    cast(extract(time from timestamp_seconds({{ faixa_horaria * 60 }} * div(unix_seconds(timestamp(timestamp_gps)), {{ faixa_horaria * 60 }}))) as string) faixa_horaria,
    case
        when status_tipo_parada = 'garagem' then 'garagem'
        else 'operando'
    end situacao
from {{ registros }}
WHERE data BETWEEN DATE({{ date_range_start }}) AND DATE({{ date_range_end }})