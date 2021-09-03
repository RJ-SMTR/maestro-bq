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
                DATE(data_versao) AS data_versao,
        from `rj-smtr.br_rj_riodejaneiro_sigmob.shapes` s
        where date(data_versao) between "2021-08-24" and "2021-08-30"
    ),
    pts as (
    -- CONSTRUCT POINT GEOGRAPHIES 
        SELECT * except(shape_pt_lon, shape_pt_lat), 
        st_geogpoint(shape_pt_lon, shape_pt_lat) as ponto_shape,
        row_number() over (partition by data_versao, shape_id order by shape_pt_sequence) rn
        FROM contents
        order by data_versao, shape_id, shape_pt_sequence
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
        select c1.shape_id,
                c1.ponto_shape start_pt,
                c2.ponto_shape end_pt,
                c1.data_versao
        from (select * from pts where shape_pt_sequence = 1) c1
        join (select * from pts where rn = 1) c2
        on c1.shape_id = c2.shape_id and c1.data_versao = c2.data_versao
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
    STRUCT("f50784f306eeb6caedbb97097582a3911b85c789" AS versao_maestro, "d472eda4529965e2d01507a0fa2757b7d16c9d48" AS versao_maestro_bq) versao
from merged m 
join linhas l
on m.shape_id = l.trip_id
and m.data_versao = l.data_versao