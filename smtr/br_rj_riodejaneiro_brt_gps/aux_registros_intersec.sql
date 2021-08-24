/*View para calcular quais linhas tem alguma correspondência com as trajetórias capturas dos veículos em circulação*/
with shapes as (
/*
GEOGRAPHY type geometry for shapes
*/
select *
from `rj-smtr-dev.br_rj_riodejaneiro_sigmob.shapes_geom` -- Apontar para a tabela a ser criada em prod
),
registros as (
/*
Generate ponto_carro for GEOGRAPHY operations
*/
select *, ST_GEOGPOINT(longitude, latitude) ponto_carro
from `rj-smtr-dev.pytest.brt_registros_tratada_1_dia` -- Apontar para a tabela base a ser definida 
),
times as (
/*
Generate empty table of intervals
*/
select faixa_horaria 
from (
select CAST(MIN(data) as TIMESTAMP) min_date, TIMESTAMP_ADD(CAST(MAX(data) as TIMESTAMP), interval 1 day) max_date
from registros ) r
join UNNEST(GENERATE_TIMESTAMP_ARRAY(r.min_date, r.max_date, Interval 5 minute)) faixa_horaria
),
faixas as (
/*
Join registros with intervals generated above
*/
select codigo, linha, timestamp_captura, faixa_horaria, longitude, latitude, ponto_carro, data, hora
from times t
join registros r
on (r.timestamp_captura between datetime(faixa_horaria) and datetime(timestamp_add(faixa_horaria, interval 5 minute)))
),
intersects as (
/*
Count number of intersects between vehicle and informed route shape
*/
select codigo as vehicle_id, 
       f.linha as linha_gps,
       s.linha_gtfs,
       shape_distance as distance,
       data, hora, faixa_horaria, s.shape_id as trip_id,
       min(timestamp_captura) as timestamp_inicio,
       count(timestamp_captura) as total_count,
       count(case when st_dwithin(ponto_carro, shape, 100) then 1 end) n_intersec,
       case
           when count(case when st_dwithin(start_pt, ponto_carro, 100) is true then 1 end)>=1 then 'start'
           when count(case when st_dwithin(end_pt, ponto_carro, 100) is true then 1 end)>=1 then 'end'
           else 'middle' end as status
from faixas f
join shapes s
on 1=1
group by codigo, faixa_horaria, linha_gps, linha_gtfs, trip_id, data, hora, distance
)
select * from intersects
where n_intersec>0 
order by vehicle_id, trip_id, faixa_horaria, n_intersec
