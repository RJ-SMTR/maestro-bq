with frota as (
    select 
        data, 
        linha, 
        faixa_horaria,
        count(distinct id_veiculo) frota_aferida
    from {{ detalhes_veiculo_onibus_completa }}
    where
    data between DATE({{ date_range_start }}) and DATE({{ date_range_end }}) 
    and DATETIME(concat(cast(data as string), " ", faixa_horaria)) > {{ date_range_start }} 
    and DATETIME(concat(cast(data as string), " ", faixa_horaria)) <= {{ date_range_end }}
    and situacao = 'operando'
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
    select 
        DATE(f.data_versao) data_versao,
        route_short_name,
        sum(FrotaServico) frota_servico,
        consorcio
    from 
        {{ frota_determinada }} f
    join (
        select  
            route_id, 
            agency_id,
            route_short_name,
            data_versao
        from {{ routes }}
    ) r
    on 
        f.route_id = r.route_id
    and 
        DATE(f.data_versao) = r.data_versao
    join (
        select 
            agency_id,
            Normalize_and_Casefold(json_value(content, "$.agency_name")) consorcio,
            DATE(data_versao) as data_versao
    from {{ agency }}
    ) c
    on
        r.agency_id = c.agency_id
    and
        r.data_versao = c.data_versao
    group by 
        f.data_versao, route_short_name, consorcio
),
frota_consorcio as (
    SELECT
        f1.*,
        f2.*,
        CASE
            WHEN consorcio = "intersul"
            THEN
                CASE    
                WHEN TIME(f1.faixa_horaria) between TIME(6,0,0) and TIME(8,50,0)
                THEN 'manha'
                WHEN TIME(f1.faixa_horaria) between TIME(16,0,0) and TIME(18,50,0)
                THEN 'tarde'
                ELSE 'fora pico'
                END
            WHEN consorcio = 'internorte'
            THEN
                CASE    
                WHEN TIME(f1.faixa_horaria) between TIME(5,30,0) and TIME(8,20,0)
                THEN 'manha'
                WHEN TIME(f1.faixa_horaria) between TIME(16,0,0) and TIME(18,50,0)
                THEN 'tarde'
                ELSE 'fora pico'
                END
            WHEN consorcio = 'transcarioca'
            THEN
                CASE    
                WHEN TIME(f1.faixa_horaria) between TIME(5,30,0) and TIME(8,20,0)
                THEN 'manha'
                WHEN TIME(f1.faixa_horaria) between TIME(16,0,0) and TIME(18,50,0)
                THEN 'tarde'
                ELSE 'fora pico'
                END
            WHEN consorcio = "santa cruz"
            THEN
                CASE    
                WHEN TIME(f1.faixa_horaria) between TIME(5,0,0) and TIME(7,50,0)
                THEN 'manha'
                WHEN TIME(f1.faixa_horaria) between TIME(17,0,0) and TIME(19,50,0)
                THEN 'tarde'
                ELSE 'fora pico'
                END
        END pico
    FROM (
        SELECT
        t1.*,
        t2.data_versao_efetiva_frota_determinada as data_versao_efetiva
        FROM frota_completa t1
        JOIN {{ data_versao_efetiva }} t2
        on t1.data = t2.data
        ) f1
    join 
        frota_sigmob f2
    on 
        f1.linha = f2.route_short_name
    and 
        f1.data_versao_efetiva = f2.data_versao

)
select 
    t1.linha,
    t1.data data,
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
from frota_consorcio t1
join capturas_por_faixa_horaria t2
on t1.faixa_horaria = t2.faixa_horaria
and t1.data = t2.data
