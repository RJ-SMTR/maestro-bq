SELECT 
    data,
    EXTRACT(HOUR FROM hora_completa) hora,
    consorcio,
    count(distinct id_veiculo) n_veiculos
from {{ registros_tratada }} t1
join {{ linhas_sppo }} t2
on t1.linha = t2.linha_completa
where not t1.status_tipo_parada = "garagem"
and t1.data between {{ date_range_start }} and {{ date_range_end }}
group by 
    t1.data,
    EXTRACT(HOUR FROM hora_completa),
    t2.consorcio