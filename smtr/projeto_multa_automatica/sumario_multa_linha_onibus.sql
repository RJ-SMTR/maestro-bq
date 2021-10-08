with multa_ultima_hora as (
    select * except (row_num)
    from (
        select *, row_number() over (
            partition by linha, data, pico, tipo_multa 
            order by linha, data, pico, tipo_multa, faixa_horaria  DESC) row_num
        from {{ detalhes_multa_linha_onibus }}
        where data between date({{ date_range_start }}) and date({{ date_range_end }})
        and 
        DATETIME(concat(cast(data as string), " ", faixa_horaria)) between {{ date_range_start }} and {{ date_range_end }}
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
left join {{ linhas }} t2
on t1.linha = t2.linha_completa