create or replace view `rj-smtr-dev.projeto_multa_automatica.view_detalhes_multa_veiculo_onibus` as
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
    cast(t1.data as string) data,
    cast(t1.faixa_horaria as string) faixa_horaria,
    t1.situacao,
    t2.tipo_multa,
    t2.artigo_multa,
    row_number() over (partition by t1.linha, t1.data, t1.faixa_horaria, t2.tipo_multa order by t1.linha, t1.data, t1.faixa_horaria, t2.tipo_multa, t3.ordem) as row_num
from `rj-smtr-dev.projeto_multa_automatica.view_detalhes_veiculo_onibus_completa` t1
join `rj-smtr-dev.projeto_multa_automatica.view_detalhes_multa_linha_onibus` t2
on t1.linha = t2.linha 
and t1.data = t1.data
and t1.faixa_horaria = t2.faixa_horaria
join categorias t3
on t1.situacao = t3.situacao)
where row_num = 1