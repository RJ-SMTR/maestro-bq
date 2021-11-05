/* 
Descrição: Consolida as infrações passíveis de multa por linha e horário de pico (manhã/tarde)
*/
with multa_ultima_hora as (
    -- 1. Seleciona as multas mais recente para
    --    cada linha de ônibus e horário de pico (manhã ou tarde)
    select * except (row_num)
    from (
        select *, row_number() over (
            partition by linha, data, pico, tipo_multa 
            order by linha, data, pico, tipo_multa, faixa_horaria  DESC) row_num
        from {{ detalhes_multa_linha_onibus }}
        where
            DATE(data) between date({{ date_range_start }}) and date({{ date_range_end }})
            and datetime(concat(cast(data as string), " ", faixa_horaria)) > {{ date_range_start }}
            and datetime(concat(cast(data as string), " ", faixa_horaria)) <= {{ date_range_end }}
        ) 
    where row_num = 1
),
linhas as (
select 
    replace(route_short_name, " ", "") linha,
    Vista vista,
    agency_name consorcio,
    data_versao,
from {{ routes }}
)
-- 2. Seleciona as multas mais recente para
--    cada linha de ônibus e horário de pico (manhã ou tarde)
select 
    t1.id_multa,
    t1.linha,
    t2.vista,
    t2.consorcio,
    t1.data,
    case extract(dayofweek from date(data))
        when 1 then 'Domingo'
        when 7 then 'Sábado'
        else 'Dia Útil' 
    end tipo_dia,
    pico,
    faixa_horaria,
    t1.frota_servico,
    frota_minima,
    frota_aferida,
    porcentagem_frota,
    tipo_multa,
    artigo_multa, 
    row_number() over (
        partition by t1.linha, data, pico
        order by t1.linha, data, pico, prioridade) prioridade
from (
select 
    m.*,
    data_versao_efetiva_routes data_versao_efetiva
from multa_ultima_hora m
join {{ data_versao_efetiva }} d
on m.data = d.data
) t1
left join linhas t2
on t1.linha = t2.linha
and t1.data_versao_efetiva = t2.data_versao;