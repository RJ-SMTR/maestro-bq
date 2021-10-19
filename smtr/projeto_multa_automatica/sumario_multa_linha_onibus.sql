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
        where data > date({{ date_range_start }}) 
        and data <= date({{ date_range_end }})
        ) 
    where row_num = 1
    )
-- 2. Seleciona as multas mais recente para
--    cada linha de ônibus e horário de pico (manhã ou tarde)
select 
    t1.id_multa,
    t1.linha,
    t2.vista,
    t2.consorcio,
    data,
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
from multa_ultima_hora t1
left join {{ linhas_sppo }} t2
on t1.linha = t2.linha_completa