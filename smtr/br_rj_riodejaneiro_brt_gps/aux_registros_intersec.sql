with 
    shapes as (
        --Buffer shapes to compensate gps precision, buffer boundary points to detect start/end of trip
        select *
        from {{ shapes }}
    ),
    registros as (
        --Generate ponto_carro for GEOG operations
        select *, ST_GEOGPOINT(longitude, latitude) ponto_carro
        from {{ registros_filtrada }} 
    ),
    times as (
        --Generate empty table of intervals
        select faixa_horaria 
        from (
        select CAST(MIN(data) as TIMESTAMP) min_date, TIMESTAMP_ADD(CAST(MAX(data) as TIMESTAMP), INTERVAL 1 day) max_date
        from registros) r
        join UNNEST(GENERATE_TIMESTAMP_ARRAY(r.min_date, r.max_date, INTERVAL {{ faixa_horaria_minutos }} minute)) faixa_horaria
    ),
    faixas as (
        --Join registros with intervals generated above
        SELECT 
            id_veiculo,
            placa_veiculo, 
            linha, 
            timestamp_captura, 
            faixa_horaria, 
            longitude, 
            latitude,
            ponto_carro, 
            data, 
            hora
        FROM times t
        JOIN registros r
        ON (r.timestamp_captura BETWEEN DATETIME(faixa_horaria) AND DATETIME(timestamp_add(faixa_horaria, interval {{ faixa_horaria_minutos }} minute)))
    ),
    intersects as (
        --Count number of intersects between vehicle and informed route shape
        SELECT 
            id_veiculo,
            placa_veiculo, 
            f.linha as linha_gps,
            s.linha_gtfs, 
            shape_distance as distancia,
            data, 
            hora, 
            faixa_horaria, 
            s.shape_id as trip_id,
            min(timestamp_captura) as timestamp_inicio,
            count(timestamp_captura) as total_capturas,
            count(case when st_dwithin(ponto_carro, shape, {{ tamanho_buffer_metros}}) then 1 end) n_intersec,
            CASE
                WHEN count(case when st_dwithin(start_pt,ponto_carro,{{ tamanho_buffer_metros }}) is true then 1 end)>=1 THEN 'start'
                WHEN count(case when st_dwithin(end_pt, ponto_carro, {{ tamanho_buffer_metros }}) is true then 1 end)>=1 THEN 'end'
                ELSE 'middle' END AS status
    FROM faixas f
    JOIN shapes s
    ON 
        s.data_versao = f.data 
    AND 
        f.linha = linha_gtfs
    GROUP by 
        id_veiculo, placa_veiculo,faixa_horaria, linha_gps, linha_gtfs, trip_id, data, hora, distancia
    )
select *,
        STRUCT({{ maestro_sha }} AS versao_maestro, {{ maestro_bq_sha }} AS versao_maestro_bq) versao
from intersects
where n_intersec>0 
order by id_veiculo, trip_id, faixa_horaria, n_intersec
