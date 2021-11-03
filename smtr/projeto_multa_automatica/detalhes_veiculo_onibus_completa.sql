/*
 Descrição: Puxa os dados de GPS do SPPO por timestamp, cria a faixa
 horária (10 minutos) e coluna de situação da operação (em
 operação/garagem) 
*/

with 
gps as (
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
and timestamp_gps <= {{ date_range_end }}
),
efetiva as (
select 
    g.*, 
    data_versao_efetiva
from gps g
join (
    select 
        data,
        data_versao_efetiva_holidays as data_versao_efetiva
  from {{ data_versao_efetiva }}
  ) d
  on g.data = d.data
),
holidays as (
select 
    array_agg(data) as feriados, 
    data_versao 
from {{ holidays }}
group by data_versao
)
select 
    e.* except(data_versao_efetiva)
from efetiva e
join holidays f
on e.data_versao_efetiva = date(f.data_versao)
where e.data not in unnest(f.feriados)