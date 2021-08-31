-- get start/end of trips, detects vehicles coming in the buffer at each boundary point
with 
    t as (
        select id_veiculo,linha_gps,linha_gtfs,
            round(distancia,1) distancia, trip_id, faixa_horaria, status,
            data, hora, 
            string_agg(status,"") over (
                partition by id_veiculo, trip_id
                order by id_veiculo, trip_id, faixa_horaria
                rows between current row and 1 following) = 'startmiddle' starts,
            string_agg(status,"") over (
                partition by id_veiculo, trip_id
                order by id_veiculo, trip_id, faixa_horaria
                rows between 1 preceding and current row) = 'middleend' ends
        from {{ intersec }}
        where data between DATE({{ date_range_start }}) and DATE({{ date_range_end }}) 
        and linha_gps=linha_gtfs
        order by trip_id, faixa_horaria
    ),
    s as (
        select *,
            case when
            string_agg(status,"") over (
                partition by id_veiculo, trip_id
                order by id_veiculo, trip_id, faixa_horaria
                rows between current row and 1 following) = 'startend' 
            then datetime(faixa_horaria, "America/Sao_Paulo") end datetime_partida,
            case when string_agg(status,"") over (
                partition by id_veiculo, trip_id
                order by id_veiculo, trip_id, faixa_horaria
                rows between 1 preceding and current row) = 'startend' 
            then datetime(faixa_horaria,"America/Sao_Paulo") end datetime_chegada
        from t
        where starts = true or ends = true
    ),
    w as (
        select * except(datetime_partida), 
            lag(datetime_partida) over(
            partition by id_veiculo, trip_id 
            order by id_veiculo, trip_id, faixa_horaria) datetime_partida,
        from s
    ),
    realized_trips as (
        select *,
            1 as tipo_trajeto,
            datetime_diff(datetime_chegada, datetime_partida, minute) as tempo_gasto, 
            round(SAFE_DIVIDE(distancia/1000, datetime_diff(datetime_chegada, datetime_partida, minute)/60), 1) as velocidade_trajeto,
            STRUCT({{ maestro_sha }} AS versao_maestro, {{ maestro_bq_sha }} AS versao_maestro_bq) versao
        from w
        where datetime_partida is not null
        order by id_veiculo, linha_gtfs, trip_id, datetime_partida
    )
select * from realized_trips 
where velocidade_trajeto between {{ filtro_min_velocidade }} and {{ filtro_max_velocidade }}