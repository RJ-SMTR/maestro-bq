with frota as (
    select 
        data, 
        linha, 
        faixa_horaria,
        count(distinct id_veiculo) frota_aferida
    from {{ detalhes_veiculo_onibus_completa }}
    where situacao = 'operando'
    and linha is not null
    group by data, linha, faixa_horaria),
combinacoes as (
    select 
        extract(date from faixa_horaria) data,
        linha, 
        extract(time from faixa_horaria) faixa_horaria
    from (
        select cast(date_add(max(data), interval 1 day) as timestamp) max_data, cast(min(data) as timestamp) min_data
        from frota),
    unnest(generate_timestamp_array(min_data, max_data, interval {{ faixa_horaria }} minute)) faixa_horaria
    cross join (select distinct linha from frota where linha is not null) 
),
frota_completa as (
    select 
        c.data, c.linha, c.faixa_horaria, 
        case 
            when extract(hour from c.faixa_horaria) between {{ hora_pico["manha"]["inicio"] }} and {{ hora_pico["manha"]["fim"] }} then 'manha' 
            when extract(hour from c.faixa_horaria) between {{ hora_pico["noite"]["inicio"] }} and {{ hora_pico["noite"]["fim"] }} then 'tarde'
            else 'fora pico'
        end pico,
        coalesce(f.frota_aferida, 0) frota_aferida
    from frota f
    full outer join combinacoes c
    on cast(f.faixa_horaria as time) = c.faixa_horaria
    and f.linha = c.linha
    and f.data = c.data
    order by c.data, c.linha, c.faixa_horaria
),
capturas_por_faixa_horaria as (
    select 
        date(faixa_horaria) data,
        time(faixa_horaria) faixa_horaria,
        capturas,
        case when sucessos <= {{ n_minimo_sucessos_api }} then true else false end flag_falha_api, 
        case when capturas <= {{ n_minimo_sucessos_captura }} then true else false end flag_falha_capturas_smtr
    from (
    select 
        timestamp_seconds({{ faixa_horaria * 60 }} * div(unix_seconds(timestamp(timestamp_captura)), {{ faixa_horaria * 60 }})) faixa_horaria,
        count(distinct timestamp_captura) capturas,
        sum(case when sucesso = true then 1 else 0 end) sucessos
    from {{ registros_logs }}
    group by timestamp_seconds({{ faixa_horaria * 60 }} * div(unix_seconds(timestamp(timestamp_captura)), {{ faixa_horaria * 60 }})))
    order by 1,2
),
frota_sigmob as (
    select data_versao data, route_short_name, sum(frota_servico) frota_servico
    from {{ frota_determinada }}
    group by data_versao, route_short_name
)
select 
    t1.linha,
    cast(t1.data as string) data,
    cast(t1.faixa_horaria as string) faixa_horaria,
    pico,
    frota_aferida,
    frota_servico,
    case 
        when frota_servico <= {{ limiar_frota_determinada }} then frota_servico                              # até 5 carros 
        when extract(dayofweek from t1.data) = 1 then  floor(frota_servico * {{ proporcao_domingo }})   # domingo
        when extract(dayofweek from t1.data) = 7 then  floor(frota_servico * {{ proporcao_sabado }})   # sábado
        else floor(frota_servico * {{ proporcao_dia_util }})                                             # dias úteis
    end frota_minima,
    SAFE_DIVIDE(frota_aferida, frota_servico) porcentagem_frota,
    case 
        when frota_servico <= {{ limiar_frota_determinada }} and frota_servico > frota_aferida then true                              # até 5 carros 
        when extract(dayofweek from t1.data) = 1 and  floor(frota_servico * {{ proporcao_domingo }}) > frota_aferida then true # domingo
        when extract(dayofweek from t1.data) = 7 and  floor(frota_servico * {{ proporcao_sabado }}) > frota_aferida then true # sábado
        when floor(frota_servico * {{ proporcao_dia_util }}) > frota_aferida then true                                          # dias úteis
        else false
    end flag_irregular,
    frota_aferida = 0 flag_sem_carros,
    flag_falha_api,
    flag_falha_capturas_smtr
from frota_completa t1
join frota_sigmob t2
on t1.linha = t2.route_short_name
and t1.data = t2.data
join capturas_por_faixa_horaria t3
on t1.faixa_horaria = t3.faixa_horaria
and t1.data = t3.data
