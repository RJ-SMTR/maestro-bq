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
      "field": "data_versao",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = "data_versao",
    partitions = partitions_to_replace,
  )
}}

with
       trips as (
              SELECT 
                     trip_id,
                     json_value(t.content,"$.route_id") route_id,
                     DATE(data_versao) data_versao
              FROM {{ var('trips') }} t
              {% if is_incremental() %}
                  where DATE(t.data_versao) in (
                  {{ partitions_to_replace | join(', ') }}
                  )
              {% endif %}
       ),
       linhas as (
              SELECT 
                     trip_id, t.route_id, 
                     json_value(r.content, "$.route_short_name") linha, 
                     json_value(r.content, "$.idModalSmtr") id_modal_smtr,
                     t.data_versao,
              FROM trips t
              INNER JOIN (
              SELECT *
              FROM {{ var('routes') }}
              {% if is_incremental() %}
                  where DATE(data_versao) in (
                  {{ partitions_to_replace | join(', ') }}
                  )
              {% endif %}
              ) r
              on t.route_id = r.route_id and t.data_versao = r.data_versao
       ),
       contents as (
       -- EXTRACTS VALUES FROM JSON STRING FIELD 'content' 
              SELECT shape_id,
              SAFE_CAST(json_value(content, "$.shape_pt_lat") AS FLOAT64) shape_pt_lat,
              SAFE_CAST(json_value(content, "$.shape_pt_lon") AS FLOAT64) shape_pt_lon,
              SAFE_CAST(json_value(content, "$.shape_pt_sequence") as INT64) shape_pt_sequence,
              DATE(data_versao) AS data_versao,
              FROM {{ var('shapes') }} s
              {% if is_incremental() %}
                  where DATE(data_versao) in (
                  {{ partitions_to_replace | join(', ') }}
                  )
              {% endif %}
       ),
       pts as (
              -- CONSTRUCT POINT GEOGRAPHIES 
              SELECT * except(shape_pt_lon, shape_pt_lat), 
              st_geogpoint(shape_pt_lon, shape_pt_lat) as ponto_shape,
              row_number() over (partition by data_versao, shape_id order by shape_pt_sequence DESC) rn
              FROM contents
              ORDER BY data_versao, shape_id, shape_pt_sequence
       ),
       shapes as (
              -- BUILD LINESTRINGS OVER SHAPE POINTS
              SELECT 
                     shape_id, 
                     data_versao,
                     st_makeline(ARRAY_AGG(ponto_shape)) as shape
              FROM pts
              GROUP BY data_versao, shape_id
       ),
       boundary as (
              -- EXTRACT START AND END POINTS FROM SHAPES
              SELECT c1.shape_id,
                     c1.ponto_shape start_pt,
                     c2.ponto_shape end_pt,
                     c1.data_versao
              FROM (select * from pts where shape_pt_sequence = 1) c1
              JOIN (select * from pts where rn = 1) c2
              ON c1.shape_id = c2.shape_id and c1.data_versao = c2.data_versao
       ),
       merged as (
              -- JOIN SHAPES AND BOUNDARY POINTS
              SELECT s.shape_id, shape, 
                     round(ST_LENGTH(shape),1) shape_distance,
                     start_pt,
                     end_pt,
                     s.data_versao
              FROM shapes s
              JOIN boundary b
              ON s.shape_id = b.shape_id and s.data_versao = b.data_versao
       )
SELECT 
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
       STRUCT("lalala" AS versao_maestro, "lelele" AS versao_maestro_bq) versao
FROM merged m 
JOIN linhas l
ON m.shape_id = l.trip_id
AND m.data_versao = l.data_versao