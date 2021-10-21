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
Descrição: Detalha todos os registros de infrações por linha, faixa horária e situação da operação (operando/garagem)
*/
with categorias as (
    SELECT situacao, offset ordem
    FROM UNNEST(['operando', 'garagem'])
    AS situacao
    WITH OFFSET AS offset
    ORDER BY offset
)
select * except(row_num)
from (
select 
    t1.linha,
    t1.data,
    cast(t1.faixa_horaria as string) faixa_horaria,
    t1.situacao,
    t2.tipo_multa,
    t2.artigo_multa,
    row_number() over (partition by t1.linha, t1.data, t1.faixa_horaria, t2.tipo_multa order by t1.linha, t1.data, t1.faixa_horaria, t2.tipo_multa, t3.ordem) as row_num
from (
    select * from {{ ref('detalhes_veiculo_onibus_completa') }}
    {% if is_incremental() %}
    WHERE
        DATE(data) in ({{ partitions_to_replace | join(', ') }})
        and datetime(concat(cast(data as string), " ", faixa_horaria)) in ({{ partitions_to_replace | join(', ') }})
    {% endif %}
) t1
join (
    select *
    from {{ ref('detalhes_multa_linha_onibus') }}
    {% if is_incremental() %}
    WHERE
        DATE(data) in ({{ partitions_to_replace | join(', ') }})
        and datetime(concat(cast(data as string), " ", faixa_horaria)) in ({{ partitions_to_replace | join(', ') }})
    {% endif %}
) t2
on t1.linha = t2.linha 
and t1.data = t1.data
and t1.faixa_horaria = t2.faixa_horaria
join categorias t3
on t1.situacao = t3.situacao)
where row_num = 1