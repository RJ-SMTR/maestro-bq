SELECT 
    data,
    EXTRACT(HOUR FROM timestamp_gps) hora,
    consorcio,
    count(distinct id_veiculo) n_veiculos
from {{ gps_sppo }} t1
join {{ linhas_sppo }} t2
on t1.servico = t2.linha_completa
where not t1.tipo_parada = "garagem"
and t1.data between {{ date_range_start }} and {{ date_range_end }}
group by 
    t1.data,
    EXTRACT(HOUR FROM timestamp_gps),
    t2.consorcio