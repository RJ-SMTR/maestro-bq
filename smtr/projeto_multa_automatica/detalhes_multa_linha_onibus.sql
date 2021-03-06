/*
Descrição: Registra todas as infrações por faixa horária de acordo com as regras estabelecidas.
*/
with filtrada as (
    -- 1. Filtra infrações em horários de pico e sem falhas de captura
    select *
    from {{ detalhes_linha_onibus_completa }}
    where
    DATE(data) between date({{ date_range_start }}) and date({{ date_range_end }})
    and datetime(concat(cast(data as string), " ", faixa_horaria)) > {{ date_range_start }}
    and datetime(concat(cast(data as string), " ", faixa_horaria)) <= {{ date_range_end }}
    and pico != 'fora pico'
    and flag_falha_api = False
    and flag_falha_capturas_smtr = False
    
),
multa_nao_consecutiva as (
    -- 2. Registra multa por faixa horária não consecutiva
    select 
    *,
    countif(flag_irregular) over (
        partition by linha, data, pico, flag_irregular
        order by linha, data, pico, faixa_horaria
    rows between unbounded preceding and current row) >= {{ multa_nao_consecutiva["valor"] }} flag_multa,
    "{{ multa_nao_consecutiva["descricao"] }}" tipo_multa,
    "{{ multa_nao_consecutiva["artigo"] }}" artigo_multa,
    {{ multa_nao_consecutiva["prioridade"] }} prioridade 
from filtrada),
multa_consecutiva as (
    -- 3. Registra multa por faixa horária consecutiva
    select
    *,
    GREATEST(
        countif(flag_irregular) over (
            partition by linha, data, pico 
            order by linha, data, pico, faixa_horaria
            rows between {{ ((multa_consecutiva["valor"] - 1) / 2)|int }} preceding and {{ ((multa_consecutiva["valor"] - 1) / 2)|int}} following),
        countif(flag_irregular) over (
            partition by linha, data, pico 
            order by linha, data, pico, faixa_horaria
            rows between {{ multa_consecutiva["valor"] - 1 }} preceding and current row),
        countif(flag_irregular) over (
            partition by linha, data, pico 
            order by linha, data, pico, faixa_horaria
            rows between current row and {{ multa_consecutiva["valor"] - 1 }} following)
    ) = {{ multa_consecutiva["valor"] }} flag_multa,
    "{{ multa_consecutiva["descricao"] }}" tipo_multa,
    "{{ multa_consecutiva["artigo"] }} "artigo_multa,
    {{ multa_consecutiva["prioridade"] }} prioridade 
from filtrada),
multa_X_horas_sem_carros as (
    -- 4. Registra multa por tempo de operação sem carros (não considerado)
    select
    *,
    GREATEST(
        countif(flag_sem_carros) over (
            partition by linha, data, pico 
            order by linha, data, pico, faixa_horaria
            rows between {{ ((multa_horas_sem_carros["valor"] - 1) / 2)|int }} preceding and {{ ((multa_horas_sem_carros["valor"] - 1) / 2)|int }} following),
        countif(flag_sem_carros) over (
            partition by linha, data, pico 
            order by linha, data, pico, faixa_horaria
            rows between {{ multa_horas_sem_carros["valor"] }} preceding and current row),
        countif(flag_sem_carros) over (
            partition by linha, data, pico 
            order by linha, data, pico, faixa_horaria
            rows between current row and {{ multa_horas_sem_carros["valor"] }} following)
    ) >= {{ multa_horas_sem_carros["valor"] }} flag_multa,
    "{{ multa_horas_sem_carros["descricao"] }}" tipo_multa,
    "{{ multa_horas_sem_carros["artigo"] }}" artigo_multa,
    1 prioridade 
from filtrada),
geral as (
    select * from multa_nao_consecutiva
    union all
    select * from multa_consecutiva
    union all
    select * from multa_X_horas_sem_carros)
select 
    concat(replace(cast(data as string), '-', ''), '-', linha, '-', pico,'-', prioridade) id_multa,
    linha,
    data,
    tipo_dia,
    pico,
    faixa_horaria,
    frota_aferida,
    frota_servico,
    frota_minima,
    porcentagem_frota, 
    tipo_multa,
    artigo_multa,
    prioridade
from geral 
where flag_multa = True
