with filtrada as (
    select *
    from {{ detalhes_veiculo_linha_completa }}
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
    rows between unbounded preceding and unbounded following) >= {{ multa_nao_consecutiva["valor"] }} flag_multa,
    "{{ multa_nao_consecutiva["descricao"] }}" tipo_multa,
    "{{ multa_nao_consecutiva["artigo"] }}" artigo_multa,
    {{ multa_nao_consecutiva["prioridade"] }} prioridade 
from filtrada),
multa_3_consecutivas as (
select
    *,
    # calcula se há 3 irregularidades consecutivas
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
    select * from multa_8_irregularidades
    union all
    select * from multa_3_consecutivas
    union all
    select * from multa_X_horas_sem_carros)
select 
    concat(replace(data, '-', ''), '-', linha, '-', pico,'-', prioridade) id_multa,
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
order by linha, faixa_horaria
