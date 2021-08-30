with t as (
    select id_veiculo,linha_gps,linha_gtfs, round(distancia,1) distancia, trip_id, faixa_horaria, status,
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
    order by trip_id, faixa_horaria),
s as (
select *,
    case when
    string_agg(status,"") over (
        partition by id_veiculo, trip_id
        order by id_veiculo, trip_id, faixa_horaria
        rows between current row and 1 following) = 'startend' 
    then datetime(faixa_horaria, "America/Sao_Paulo") end departure_time,
    case when string_agg(status,"") over (
        partition by id_veiculo, trip_id
        order by id_veiculo, trip_id, faixa_horaria
        rows between 1 preceding and current row) = 'startend' 
    then datetime(faixa_horaria,"America/Sao_Paulo") end arrival_time
from t
where starts = true or ends = true),
w as (
select id_veiculo,linha_gps, linha_gtfs, distance, trip_id, 
    lag(departure_time) over(
    partition by id_veiculo, trip_id 
    order by id_veiculo, trip_id, faixa_horaria) departure_time,
    arrival_time,
from s),
realized_trips as (
select *,
       1 as trajectory_type,
       datetime_diff(arrival_time, departure_time, minute) as elapsed_time, 
       round(SAFE_DIVIDE(distance/1000, datetime_diff(arrival_time, departure_time, minute)/60), 1) as average_speed
from w
where departure_time is not null
order by id_veiculo, linha_gtfs, trip_id, departure_time)
select * from realized_trips 
where average_speed between {{ filtro_min_velocidade }} and {{ filtro_max_velocidade }}