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
    select * from {{ detalhes_veiculo_onibus_completa }}
    where
    DATE(data) between date({{ date_range_start }}) and date({{ date_range_end }})
    and datetime(concat(cast(data as string), " ", faixa_horaria)) > {{ date_range_start }}
    and datetime(concat(cast(data as string), " ", faixa_horaria)) <= {{ date_range_end }}
) t1
join (
    select *
    from {{ detalhes_multa_linha_onibus }}
    where
    DATE(data) between date({{ date_range_start }}) and date({{ date_range_end }})
    and datetime(concat(cast(data as string), " ", faixa_horaria)) > {{ date_range_start }}
    and datetime(concat(cast(data as string), " ", faixa_horaria)) <= {{ date_range_end }}
) t2
on t1.linha = t2.linha 
and t1.data = t1.data
and t1.faixa_horaria = t2.faixa_horaria
join categorias t3
on t1.situacao = t3.situacao)
where row_num = 1;