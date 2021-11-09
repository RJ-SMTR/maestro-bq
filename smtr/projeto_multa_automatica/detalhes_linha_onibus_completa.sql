/*
Descrição: Consolida tabela de todas as linhas e faixas horárias
possíveis com frota aferida, frota mínima, flags de
captura e irregularidades definidas na metodologia.
*/
with frota as (
    -- 1. Conta frota operante (número de veículos em operação) por faixa horária de captura do gps
    select 
        data, 
        linha, 
        faixa_horaria,
        count(distinct id_veiculo) frota_aferida
    from {{ detalhes_veiculo_onibus_completa }}
    where
    DATE(data) between date({{ date_range_start }}) and date({{ date_range_end }})
    and datetime(concat(cast(data as string), " ", faixa_horaria)) > {{ date_range_start }}
    and datetime(concat(cast(data as string), " ", faixa_horaria)) <= {{ date_range_end }}
    and situacao = 'operando'
    and linha is not null
    group by data, linha, faixa_horaria),
combinacoes as (
    -- 2. Cria tabela de referência de todos as combinações de horários possíveis para
    --    todas as linhas aferidas, incluindo +1 dia à data máxima aferida.
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
    -- 3. Gera tabela completa da frota operante por faixa horária de
    --    captura do GPS, incluindo 0 veículos em horários sem sinal
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
    -- 4. Verifica o número de capturas do gps em cada faixa horária
    --    para registrar se houve falha na API original do gps (flag_falha_api) e/ou falha na
    --    captura dos dados de gps pela SMTR (flag_falha_capturas_smtr)
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
    where
    DATE(faixa_horaria) between date({{ date_range_start }}) and date({{ date_range_end }})
    and faixa_horaria > {{ date_range_start }}
    and faixa_horaria <= {{ date_range_end }}
    order by 1,2
),
frota_sigmob as (
    -- 5. 
    select 
        f.data,
        data_feriado,
        DATE(f.data_versao) data_versao,
        route_short_name,
        sum(FrotaServico) frota_servico,
        consorcio
    from (
        select * 
        from {{ frota_determinada }} f1
        join (
            select
                data,
                data_versao_efetiva_agency,
                data_versao_efetiva_frota_determinada,
                data_versao_efetiva_holidays,
                data_versao_efetiva_routes,
            from {{ data_versao_efetiva }}) d
    on 
        DATE(f1.data_versao) = d.data_versao_efetiva_frota_determinada) f
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
        DATE(f.data_versao_efetiva_routes) = r.data_versao
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
        f.data_versao_efetiva_agency = c.data_versao
    left join (
        select 
            data as data_feriado,
            data_versao
        from {{ holidays }}
        ) h
    on
        f.data_versao_efetiva_holidays = DATE(h.data_versao)
        and f.data = h.data_feriado
    group by 
        f.data, data_feriado, f.data_versao, route_short_name, consorcio
),
sigmob_combinacoes as (
    select 
        data,
        data_feriado,
        data_versao,
        route_short_name,
        frota_servico,
        consorcio,
        extract(time from faixa_horaria) faixa_horaria
    from (
        select 
            cast(date_add(max(data), interval 1 day) as timestamp) max_data,
            cast(min(data) as timestamp) min_data
        from frota_sigmob
    ),
    unnest(generate_timestamp_array(min_data, max_data, INTERVAL {{ faixa_horaria }} minute)) faixa_horaria
    join frota_sigmob f
    on f.data = extract(date from faixa_horaria)
    where frota_servico is not null
),
frotas_combinadas as (
    SELECT
        f2.data,
        f2.data_feriado,
        f2.route_short_name as linha,
        f2.faixa_horaria,
        coalesce(f1.frota_aferida,0) frota_aferida,
        frota_servico,
        consorcio,
    FROM (
        SELECT
            t1.*,
            t2.data_versao_efetiva_frota_determinada as data_versao_efetiva
        FROM frota_completa t1
        JOIN {{ data_versao_efetiva }} t2
        on t1.data = t2.data
    ) f1
    right join 
        sigmob_combinacoes f2
    on 
        f1.linha = f2.route_short_name
    and
        f1.data = f2.data
    and 
        f1.data_versao_efetiva = f2.data_versao
    and
        f1.faixa_horaria = f2.faixa_horaria
),
frota_consorcio as (
    SELECT
        f1.*,
        CASE
            {% for consorcio, picos in hora_pico.items() %}
            WHEN consorcio = '{{ consorcio }}'
            THEN
                CASE 
                {% for periodo, faixa in picos.items() %} 
                WHEN TIME(f1.faixa_horaria) between TIME(
                    {{ faixa['inicio']['hora'] }},
                    {{ faixa['inicio']['minuto'] }},
                    0) and TIME(
                    {{ faixa['fim']['hora'] }},
                    {{ faixa['fim']['minuto'] }},
                    0)
                THEN '{{ periodo }}'
                {% endfor %}
                ELSE 'fora pico'
            END
            {% endfor %}
        END pico,
        CASE
            WHEN extract(dayofweek from data) = 1 THEN 'Domingo'
            WHEN extract(dayofweek from data) = 7 THEN 'Sabado'
            WHEN data = data_feriado THEN 'Feriado'
            ELSE 'Dia Util'
        END tipo_dia
    FROM 
        frotas_combinadas f1
)
select 
    t1.linha,
    t1.data data,
    tipo_dia,
    cast(t1.faixa_horaria as string) faixa_horaria,
    pico,
    frota_aferida,
    frota_servico,
    case 
        when frota_servico <= {{ limiar_frota_determinada }} then frota_servico -- até 5 carros 
        when tipo_dia = 'Domingo' then  floor(frota_servico * {{ proporcao_domingo }}) -- domingo
        when tipo_dia = 'Feriado' then  floor(frota_servico * {{ proporcao_domingo }}) -- feriados
        when tipo_dia = 'Sabado' then  floor(frota_servico * {{ proporcao_sabado }}) -- sábado
        else floor(frota_servico * {{ proporcao_dia_util }}) -- dias úteis
    end frota_minima,
    SAFE_DIVIDE(frota_aferida, frota_servico) porcentagem_frota,
    case 
        when frota_servico <= {{ limiar_frota_determinada }} and frota_servico > frota_aferida then true -- até 5 carros 
        when tipo_dia = 'Domingo' and  floor(frota_servico * {{ proporcao_domingo }}) > frota_aferida then true -- domingo
        when tipo_dia = 'Feriado' and  floor(frota_servico * {{ proporcao_domingo }}) > frota_aferida then true -- feriados
        when tipo_dia = 'Sabado' and  floor(frota_servico * {{ proporcao_sabado }}) > frota_aferida then true -- sábado
        when floor(frota_servico * {{ proporcao_dia_util }}) > frota_aferida then true  -- dias úteis
        else false
    end flag_irregular,
    frota_aferida = 0 flag_sem_carros,
    flag_falha_api,
    flag_falha_capturas_smtr
from frota_consorcio t1
join capturas_por_faixa_horaria t2
on t1.faixa_horaria = t2.faixa_horaria
and t1.data = t2.data
