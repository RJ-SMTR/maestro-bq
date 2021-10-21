/*
Descrição: Registra multas por não operação da linha (não considerado)
*/
with multa_120_minutos as (
    select 
        *,
        countif(erro is not null) over (
            partition by data
            order by data
        rows between {{ (var('multa_gps_120_minutos')["valor"] / 2)|int }} preceding 
            and {{ (var('multa_gps_120_minutos')["valor"] / 2)|int }} following
        ) >= {{ var('multa_gps_120_minutos')["valor"] }} flag_multa,
        "{{ var('multa_gps_120_minutos')["descricao"] }}" tipo_multa,
        "{{ var('multa_gps_120_minutos')["artigo"] }}" artigo_multa
    from {{ var('onibus_registros_logs') }}
),
multa_1_dia as (
    select 
        *,
        countif(erro is not null) over (
            partition by data
            order by data
        rows between {{ (var('multa_gps_1_dia')["valor"]/2)|int }} preceding and 720 following
        ) >= {{ (var('multa_gps_1_dia')["valor"]/2)|int }} flag_multa,
        "{{ var('multa_gps_1_dia')["descricao"] }}" tipo_multa,
        "{{ var('multa_gps_1_dia')["artigo"] }}" artigo_multa
    from {{ registros_logs }}
)
select * from multa_120_minutos where flag_multa = true
union all 
select * from multa_1_dia where flag_multa = true