create or replace view `rj-smtr-dev.projeto_multa_automatica.view_detalhes_veiculo_onibus_completa` as
select distinct
    data,
    linha,
    ordem,
    extract(time from timestamp_seconds(60 * div(unix_seconds(timestamp(timestamp_gps)), 60))) timestamp_minuto,
    cast(extract(time from timestamp_seconds(600 * div(unix_seconds(timestamp(timestamp_gps)), 600))) as string) faixa_horaria,
    case
        when fora_garagem = false then 'garagem'
        else 'operando'
    end situacao
from rj-smtr-dev.projeto_multa_automatica.onibus_registros_tratada_1_dia
order by 1,2,3,4
