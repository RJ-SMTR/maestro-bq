create or replace view `rj-smtr-dev.projeto_multa_automatica.view_detalhes_multa_linha_onibus` as
with filtrada as (
    select *
    from `rj-smtr-dev.projeto_multa_automatica.view_detalhes_linha_onibus_completa`
    where pico != 'fora pico'
    and flag_falha_api = False
    and flag_falha_capturas_smtr = False
),
multa_8_irregularidades as (
    select 
    *,
    countif(flag_irregular) over (
        partition by linha, data, pico, flag_irregular
        order by linha, data, pico, faixa_horaria
    rows between unbounded preceding and unbounded following) >= 8 flag_multa,
    'pelo menos 80 minutos não consecutivos operando abaixo da frota esperada' tipo_multa,
    case 
        when frota_determinada <= 5 then 'Art.17.I Obs: Anexo VIII Art.19' 
        else 'Art.17.I'
    end artigo_multa,
    2 prioridade 
from filtrada
order by 1,2,3),
multa_3_consecutivas as (
select
    *,
    # calcula se há 3 irregularidades consecutivas
    GREATEST(countif(flag_irregular) over (
        partition by linha, data, pico 
        order by linha, data, pico, faixa_horaria
        rows between 1 preceding and 1 following),
    countif(flag_irregular) over (
        partition by linha, data, pico 
        order by linha, data, pico, faixa_horaria
        rows between 2 preceding and current row),
    countif(flag_irregular) over (
        partition by linha, data, pico 
        order by linha, data, pico, faixa_horaria
        rows between current row and 2 following)) = 3 flag_multa,
    'pelo menos 30 minutos consecutivos operando abaixo da frota esperada' tipo_multa,
    case 
        when frota_determinada <= 5 then 'Art.17.I Obs: Anexo VIII Art.19' 
        else 'Art.17.I'
    end artigo_multa,
    3 prioridade 
from filtrada
order by 1,2,3,4),
multa_4_horas_sem_carros as (
select
    *,
    GREATEST(countif(flag_sem_carros) over (
        partition by linha, data, pico 
        order by linha, data, pico, faixa_horaria
        rows between 12 preceding and 12 following),
    countif(flag_sem_carros) over (
        partition by linha, data, pico 
        order by linha, data, pico, faixa_horaria
        rows between 24 preceding and current row),
    countif(flag_sem_carros) over (
        partition by linha, data, pico 
        order by linha, data, pico, faixa_horaria
        rows between current row and 24 following)) >= 24 flag_multa,
    '4 horas consecutivas operando sem carros' tipo_multa,
    'Art.17.VII' artigo_multa,
     1 prioridade 
from filtrada
order by 1,2,3,4),
geral as (
    select * from multa_8_irregularidades
    union all
    select * from multa_3_consecutivas
    union all
    select * from multa_4_horas_sem_carros)
select 
    concat(replace(data, '-', ''), '-', linha, '-', pico,'-', prioridade) id_multa,
    linha,
    data,
    pico,
    faixa_horaria,
    frota_aferida,
    frota_determinada,
    frota_minima,
    porcentagem_frota, 
    tipo_multa,
    artigo_multa,
    prioridade
from geral 
where flag_multa = True
order by linha, faixa_horaria
