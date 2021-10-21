{% 
  set partitions_to_replace = [
    'date(current_date("America/Sao_Paulo"))',
    'date(date_sub(current_date("America/Sao_Paulo"), interval 1 day))'
  ]
%}

{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      "field": "data",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["data"],
    partitions = partitions_to_replace,
  )
}}

/*
Descrição: Registra todas as infrações por faixa horária de acordo com as regras estabelecidas.
*/
with filtrada as (
    -- 1. Filtra infrações em horários de pico e sem falhas de captura
    select *
    from {{ ref('detalhes_linha_onibus_completa') }}
    where
    {% if is_incremental() %}
        DATE(data) in ({{ partitions_to_replace | join(', ') }})
        and datetime(concat(cast(data as string), " ", faixa_horaria)) in ({{ partitions_to_replace | join(', ') }})
        and
    {% endif %}
        pico != 'fora pico'
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
    rows between unbounded preceding and unbounded following) >= {{ var('multa_nao_consecutiva')["valor"] }} flag_multa,
    "{{ var('multa_nao_consecutiva')["descricao"] }}" tipo_multa,
    "{{ var('multa_nao_consecutiva')["artigo"] }}" artigo_multa,
    {{ var('multa_nao_consecutiva')["prioridade"] }} prioridade 
from filtrada),
multa_consecutiva as (
    -- 3. Registra multa por faixa horária consecutiva
    select
    *,
    GREATEST(
        countif(flag_irregular) over (
            partition by linha, data, pico 
            order by linha, data, pico, faixa_horaria
            rows between {{ ((var('multa_consecutiva')["valor"] - 1) / 2)|int }} preceding and {{ ((var('multa_consecutiva')["valor"] - 1) / 2)|int}} following),
        countif(flag_irregular) over (
            partition by linha, data, pico 
            order by linha, data, pico, faixa_horaria
            rows between {{ var('multa_consecutiva')["valor"] - 1 }} preceding and current row),
        countif(flag_irregular) over (
            partition by linha, data, pico 
            order by linha, data, pico, faixa_horaria
            rows between current row and {{ var('multa_consecutiva')["valor"] - 1 }} following)
    ) = {{ var('multa_consecutiva')["valor"] }} flag_multa,
    "{{ var('multa_consecutiva')["descricao"] }}" tipo_multa,
    "{{ var('multa_consecutiva')["artigo"] }} "artigo_multa,
    {{ var('multa_consecutiva')["prioridade"] }} prioridade 
from filtrada),
multa_X_horas_sem_carros as (
    -- 4. Registra multa por tempo de operação sem carros (não considerado)
    select
    *,
    GREATEST(
        countif(flag_sem_carros) over (
            partition by linha, data, pico 
            order by linha, data, pico, faixa_horaria
            rows between {{ ((var('multa_horas_sem_carros')["valor"] - 1) / 2)|int }} preceding and {{ ((var('multa_horas_sem_carros')["valor"] - 1) / 2)|int }} following),
        countif(flag_sem_carros) over (
            partition by linha, data, pico 
            order by linha, data, pico, faixa_horaria
            rows between {{ var('multa_horas_sem_carros')["valor"] }} preceding and current row),
        countif(flag_sem_carros) over (
            partition by linha, data, pico 
            order by linha, data, pico, faixa_horaria
            rows between current row and {{ var('multa_horas_sem_carros')["valor"] }} following)
    ) >= {{ var('multa_horas_sem_carros')["valor"] }} flag_multa,
    "{{ var('multa_horas_sem_carros')["descricao"] }}" tipo_multa,
    "{{ var('multa_horas_sem_carros')["artigo"] }}" artigo_multa,
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