with 
t as (
    -- Aggregate status to detect start and end of trip
    select vehicle_id,linha_gps,linha_gtfs, round(distance,1) distance, trip_id, faixa_horaria, status,
        string_agg(status,"") over (
            partition by vehicle_id, trip_id
            order by vehicle_id, trip_id, faixa_horaria
            rows between current row and 1 following) = 'startmiddle' starts,
        string_agg(status,"") over (
            partition by vehicle_id, trip_id
            order by vehicle_id, trip_id, faixa_horaria
            rows between 1 preceding and current row) = 'middleend' ends
    from `rj-smtr-dev.br_rj_riodejaneiro_brt_gps.aux_registros_linha_intersec_no_buffer` -- Apontar para a tabela correta
    where linha_gps = linha_gtfs
    order by trip_id, faixa_horaria),
s as (
    -- Get start and end times for trips based on the previous aggregation
    select *,
        case when
        string_agg(status,"") over (
            partition by vehicle_id, trip_id
            order by vehicle_id, trip_id, faixa_horaria
            rows between current row and 1 following) = 'startend' 
        then datetime(faixa_horaria, "America/Sao_Paulo") end departure_time,
        case when string_agg(status,"") over (
            partition by vehicle_id, trip_id
            order by vehicle_id, trip_id, faixa_horaria
            rows between 1 preceding and current row) = 'startend' 
        then datetime(faixa_horaria,"America/Sao_Paulo") end arrival_time
    from t
    where starts = true or ends = true),
w as (
    -- Lags arrival_time (fetching from preceding row) to properly match trip times
    select vehicle_id,linha_gps, linha_gtfs, distance, trip_id, 
        lag(departure_time) over(
        partition by vehicle_id, trip_id 
        order by vehicle_id, trip_id, faixa_horaria) departure_time,
        arrival_time,
    from s),
realized_trips as (
    -- Define trajectory_type (1 = 'complete trip'), calculates elapsed_time and average speed over the whole trip
    select *,
        1 as trajectory_type,
        datetime_diff(arrival_time, departure_time, minute) as elapsed_time, 
        round(SAFE_DIVIDE(distance/1000, datetime_diff(arrival_time, departure_time, minute)/60), 1) as average_speed
    from w
    where departure_time is not null
    order by vehicle_id, linha_gtfs, trip_id, departure_time)

--Filter bulk data by speed, to cut off outlying results
select * 
from realized_trips 
where average_speed between 10 and 90