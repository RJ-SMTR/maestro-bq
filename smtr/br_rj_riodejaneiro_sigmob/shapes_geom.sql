with
trips as (
select trip_id,
       json_value(t.content,"$.route_id") route_id,
       
from `rj-smtr.br_rj_riodejaneiro_sigmob.trips` t
where date(t.data_versao) = date_sub(current_date(), interval 1 day)
),
linhas as (
select trip_id, t.route_id, json_value(r.content, "$.route_short_name") linha
from trips t
inner join (
select *
from `rj-smtr.br_rj_riodejaneiro_sigmob.routes`
where date(data_versao) = date_sub(current_date(), interval 1 day)) r
on t.route_id = r.route_id
),
contents as (
-- EXTRACTS VALUES FROM JSON STRING FIELD 'content' 
select shape_id,
       SAFE_CAST(json_value(content, "$.shape_pt_lat") AS FLOAT64) shape_pt_lat,
       SAFE_CAST(json_value(content, "$.shape_pt_lon") AS FLOAT64) shape_pt_lon,
       SAFE_CAST(json_value(content, "$.shape_pt_sequence") as INT64) shape_pt_sequence,
from `rj-smtr-dev.br_rj_riodejaneiro_sigmob.shapes` s
-- Adicionar filtro para data_versao correspondendo aos filtos aplicados em trips/linhas
-- where date(data_versao) = date_sub(current_date(), interval 1 day)
 ),
pts as (
-- CONSTRUCT POINT GEOGRAPHIES 
SELECT *, 
      st_geogpoint(shape_pt_lon, shape_pt_lat) as ponto_shape
FROM contents
order by shape_id, shape_pt_sequence
),
shapes as (
-- BUILD LINESTRINGS OVER SHAPE POINTS
select shape_id, 
       st_makeline(array(select ponto_shape from pts a2 where a1.shape_id = a2.shape_id )) as shape
from pts a1
group by shape_id),
boundary as (
-- EXTRACT START AND END POINTS FROM SHAPES
select t1.shape_id,
       ST_GEOGPOINT(t1.shape_pt_lon, t1.shape_pt_lat) start_pt,
       ST_GEOGPOINT(t2.shape_pt_lon, t2.shape_pt_lat) end_pt
       
from contents t1
join
(select shape_id, shape_pt_lon, shape_pt_lat,
       row_number() over (partition by shape_id order by shape_pt_sequence DESC) rn
from contents) t2
on t1.shape_id = t2.shape_id
where t2.rn = 1 and t1.shape_pt_sequence = 1
),
merged as (
-- JOIN SHAPES AND BOUNDARY POINTS, BUFFERING BOUNDARY POINTS
select s.shape_id, shape, 
       round(ST_LENGTH(shape),1) shape_distance,
       start_pt,
       end_pt,
from shapes s
join boundary b
on s.shape_id = b.shape_id)

select shape_id,
       route_id,
       replace(linha, " ", "") as linha_gtfs, 
       shape, 
       shape_distance, 
       start_pt, 
       end_pt,
       ({{ maestro-sha }} as maestro_sha, {{ maestro_bq_sha }} as maestro_bq_sha) versao_repo
from merged m 
join linhas l
on m.shape_id = l.trip_id