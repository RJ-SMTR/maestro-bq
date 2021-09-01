create or replace view rj-smtr-dev.projeto_multa_automatica.view_sumario_multa_linha_onibus as
with multa_ultima_hora as (
    select * except (row_num)
    from (
        select *, row_number() over (
            partition by linha, data, pico, tipo_multa 
            order by linha, data, pico, tipo_multa, faixa_horaria  DESC) row_num
        from rj-smtr-dev.projeto_multa_automatica.view_detalhes_multa_linha_onibus
        ) 
    where row_num = 1
    )
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
    t1.frota_determinada,
    frota_minima,
    frota_aferida,
    porcentagem_frota,
    tipo_multa,
    artigo_multa, 
    row_number() over (
        partition by t1.linha, data, pico
        order by t1.linha, data, pico, prioridade) prioridade
from multa_ultima_hora t1
left join `rj-smtr.br_rj_riodejaneiro_transporte.linhas_sppo` t2
on t1.linha = t2.linha_completa
order by 1,4,5