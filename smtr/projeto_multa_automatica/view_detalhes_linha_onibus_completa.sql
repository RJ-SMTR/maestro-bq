create or replace view `rj-smtr-dev.projeto_multa_automatica.view_detalhes_linha_onibus_completa` as
with frota as (
    select 
        data, 
        linha, 
        faixa_horaria,
        count(distinct ordem) frota_aferida
    from `rj-smtr-dev.projeto_multa_automatica.view_detalhes_veiculo_onibus_completa`
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
    unnest(generate_timestamp_array(min_data, max_data, interval 10 minute)) faixa_horaria
    cross join (select distinct linha from frota where linha is not null) 
),
frota_completa as (
    select 
        c.data, c.linha, c.faixa_horaria, 
        case 
            when extract(hour from c.faixa_horaria) between 5 and 8 then 'manha' 
            when extract(hour from c.faixa_horaria) between 16 and 19 then 'tarde'
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
        case when sucessos = 0 then true else false end flag_falha_api, 
        case when capturas <= 8 then true else false end flag_falha_capturas_smtr
    from (
    select 
        timestamp_seconds(600 * div(unix_seconds(timestamp(timestamp_captura)), 600)) faixa_horaria,
        count(distinct timestamp_captura) capturas,
        sum(case when sucesso = true then 1 else 0 end) sucessos
    from `rj-smtr.br_rj_riodejaneiro_onibus_gps.registros_logs` 
    group by timestamp_seconds(600 * div(unix_seconds(timestamp(timestamp_captura)), 600)))
    order by 1,2
)
select 
    t1.linha,
    cast(t1.data as string) data,
    cast(t1.faixa_horaria as string) faixa_horaria,
    pico,
    frota_aferida,
    frota_determinada,
    case 
        when frota_determinada <= 5 then frota_determinada                              # até 5 carros 
        when extract(dayofweek from t1.data) = 1 then  floor(frota_determinada * 0.4)   # domingo
        when extract(dayofweek from t1.data) = 7 then  floor(frota_determinada * 0.5)   # sábado
        else floor(frota_determinada * 0.8)                                             # dias úteis
    end frota_minima,
    frota_aferida / frota_determinada porcentagem_frota,
    case 
        when frota_determinada <= 5 and frota_determinada > frota_aferida then true                              # até 5 carros 
        when extract(dayofweek from t1.data) = 1 and  floor(frota_determinada * 0.4) < frota_aferida then true # domingo
        when extract(dayofweek from t1.data) = 7 and  floor(frota_determinada * 0.5) < frota_aferida then true # sábado
        when floor(frota_determinada * 0.8) < frota_aferida then true                                          # dias úteis
        else false
    end flag_irregular,
    frota_aferida = 0 flag_sem_carros,
    flag_falha_api,
    flag_falha_capturas_smtr
from frota_completa t1
join `rj-smtr-dev.projeto_multa_automatica.frota_determinada` t2
on t1.linha = t2.route_short_name
join capturas_por_faixa_horaria t3
on t1.faixa_horaria = t3.faixa_horaria
and t1.data = t3.data