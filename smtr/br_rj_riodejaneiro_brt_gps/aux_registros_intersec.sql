with shapes as (
/*
Buffer shapes to compensate gps precision, buffer boundary points to detect start/end of trip
*/
select *
from {{ shapes }}
where data_versao between DATE({{ date_range_start }}) and DATE({{ date_range_end }}) 
),
registros as (
/*
Generate ponto_carro for GEOG operations
*/
select *, ST_GEOGPOINT(longitude, latitude) ponto_carro
from {{ registros_filtrada }}
where data between DATE({{ date_range_start }}) and DATE({{ date_range_end }}) 
),
times as (
/*
Generate empty table of intervals
*/
select faixa_horaria 
from (
select CAST(MIN(data) as TIMESTAMP) min_date, TIMESTAMP_ADD(CAST(MAX(data) as TIMESTAMP), interval 1 day) max_date
from registros ) r
join UNNEST(GENERATE_TIMESTAMP_ARRAY(r.min_date, r.max_date, Interval {{ faixa_horaria }} minute)) faixa_horaria
),
faixas as (
/*
Join registros with intervals generated above
*/
select id_veiculo, linha, timestamp_captura, faixa_horaria, longitude, latitude, ponto_carro, data, hora
from times t
join registros r
on (r.timestamp_captura between datetime(faixa_horaria) and datetime(timestamp_add(faixa_horaria, interval {{ faixa_horaria }} minute)))
),
intersects as (
/*
Count number of intersects between vehicle and informed route shape
*/
    select id_veiculo, f.linha as linha_gps,s.linha_gtfs , shape_distance as distancia,
        data, hora, faixa_horaria, s.shape_id as trip_id,
        min(timestamp_captura) as timestamp_inicio,
        count(timestamp_captura) as total_capturas,
        count(case when st_dwithin(ponto_carro, shape, {{ buffer_size_meters}}) then 1 end) n_intersec,
        case
            when count(case when st_dwithin(start_pt,ponto_carro,{{ buffer_size_meters }}) is true then 1 end)>=1 then 'start'
            when count(case when st_dwithin(end_pt, ponto_carro, {{ buffer_size_meters }}) is true then 1 end)>=1 then 'end'
            else 'middle' end as status
from faixas f
join shapes s
on s.data_versao = f.data
group by id_veiculo, faixa_horaria, linha_gps, linha_gtfs, trip_id, data, hora, distancia
)
select *,
        STRUCT({{ maestro_sha }} AS versao_maestro, {{ maestro_bq_sha }} AS versao_maestro_bq) versao
from intersects
where n_intersec>0 
order by id_veiculo, trip_id, faixa_horaria, n_intersec
