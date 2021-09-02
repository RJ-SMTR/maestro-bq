-- idModal: SPPO - 22, 23, BRT- 20, 
with
       trips as (
              select trip_id,
                     json_value(t.content,"$.route_id") route_id,
                     DATE(data_versao) data_versao
                     
              from {{ trips }} t
              where DATE(t.data_versao) between DATE({{ date_range_start }}) and DATE({{ date_range_end }})
       ),
       linhas as (
              select 
                     trip_id, t.route_id, 
                     json_value(r.content, "$.route_short_name") linha, 
                     json_value(r.content, "$.idModalSmtr") id_modal_smtr,
                     t.data_versao,
              from trips t
              inner join (
              select *
              from {{ routes }}
              where DATE(data_versao) between DATE({{ date_range_start }}) and DATE({{ date_range_end }})) r
              on t.route_id = r.route_id and t.data_versao = r.data_versao
       ),
       contents as (
       -- EXTRACTS VALUES FROM JSON STRING FIELD 'content' 
              select shape_id,
                     SAFE_CAST(json_value(content, "$.shape_pt_lat") AS FLOAT64) shape_pt_lat,
                     SAFE_CAST(json_value(content, "$.shape_pt_lon") AS FLOAT64) shape_pt_lon,
                     SAFE_CAST(json_value(content, "$.shape_pt_sequence") as INT64) shape_pt_sequence,
                     DATE(data_versao) AS data_versao
              from {{ shapes }} s
              where date(data_versao) between DATE({{ date_range_start }}) and DATE({{ date_range_end }})
       ),
       pts as (
       -- CONSTRUCT POINT GEOGRAPHIES 
              SELECT *, 
              st_geogpoint(shape_pt_lon, shape_pt_lat) as ponto_shape,
              FROM contents
              order by shape_id, shape_pt_sequence
       ),
       shapes as (
       -- BUILD LINESTRINGS OVER SHAPE POINTS
              select shape_id, 
                     st_makeline(array
                     (select 
                            ponto_shape from pts a2 
                            where a1.shape_id = a2.shape_id 
                            and a1.data_versao = a2.data_versao
                            )
                     ) as shape,
                     a1.data_versao
              from pts a1
              group by shape_id, a1.data_versao
       ),
       boundary as (
              -- EXTRACT START AND END POINTS FROM SHAPES
              select shape_id,
                     CASE WHEN shape_pt_sequence = 1 THEN ST_GEOGPOINT(shape_pt_lon, shape_pt_lat) END start_pt,
                     CASE WHEN rn = 1 THEN ST_GEOGPOINT(shape_pt_lon, shape_pt_lat) END end_pt,
                     data_versao
              from (
              select shape_id, shape_pt_lon, shape_pt_lat, data_versao, shape_pt_sequence,
                     row_number() over (partition by shape_id order by shape_pt_sequence DESC) rn
              from contents )
              where rn = 1 or shape_pt_sequence = 1
       ),
       merged as (
              -- JOIN SHAPES AND BOUNDARY POINTS
              select s.shape_id, shape, 
                     round(ST_LENGTH(shape),1) shape_distance,
                     start_pt,
                     end_pt,
                     s.data_versao
              from shapes s
              join boundary b
              on s.shape_id = b.shape_id and s.data_versao = b.data_versao
       )

select 
       trip_id,
       shape_id,
       route_id,
       id_modal_smtr,
       replace(linha, " ", "") as linha_gtfs, 
       shape, 
       shape_distance, 
       start_pt, 
       end_pt,
       m.data_versao,
       STRUCT({{ maestro_sha }} AS versao_maestro, {{ maestro_bq_sha }} AS versao_maestro_bq) versao
from merged m 
join linhas l
on m.shape_id = l.trip_id
and m.data_versao = l.data_versao