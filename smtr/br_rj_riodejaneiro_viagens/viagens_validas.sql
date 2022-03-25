with viagens as (
    select 
        *,
        row_number() over (
            partition by id_veiculo, servico, shape_id
            order by datetime_partida
        ) trip_number
    from `rj-smtr-dev.br_rj_riodejaneiro_onibus_gps.viagens_realizadas`
),
merge_trip as (
    select 
        s.*,
        datetime_partida,
        datetime_chegada,
        -- trip_number
        CASE 
            WHEN 
                timestamp_gps = datetime_partida
                or
                timestamp_gps = datetime_chegada
            THEN
                trip_number
        END trip_number

    from rj-smtr-dev.br_rj_riodejaneiro_onibus_gps.aux_registros_status_viagem s
    left join viagens v 
    on s.data = v.data
    and s.id_veiculo = v.id_veiculo
    and (s.timestamp_gps = v.datetime_partida or s.timestamp_gps = v.datetime_chegada)
    and s.shape_id = v.shape_id
),
classificacao as (
    select 
        * except(trip_number),
        FIRST_VALUE(distancia/1000) over (
            partition by id_veiculo, servico, shape_id, trip_number
            order by timestamp_gps
        ) d0,
        CASE
            WHEN
                trip_number is not null
            THEN 
                trip_number
            WHEN
                (timestamp_gps >= LAST_VALUE(datetime_partida IGNORE NULLS) over (
                        partition by id_veiculo, servico, shape_id
                        order by timestamp_gps
                        rows between unbounded preceding and current row
                    )
                and
                timestamp_gps <= LAST_VALUE(datetime_chegada IGNORE NULLS) over (
                        partition by id_veiculo, servico, shape_id
                        order by timestamp_gps
                        rows between  unbounded preceding and current row
                    )
                )
            THEN 
                LAST_VALUE(trip_number IGNORE NULLS) over (
                        partition by id_veiculo, servico, shape_id
                        order by timestamp_gps
                        rows between unbounded preceding and current row
                    )
        END trip_number
    FROM merge_trip mt
),
distancia_estimada as (
    select 
        data,
        id_veiculo,
        servico,
        shape_id,
        trip_number,
        round(shape_distance/1000, 2) distancia_teorica,
        round(sum(distancia)/1000,2)  distancia_km 
    from classificacao 
    where trip_number is not null
    group by 1,2,3,4,5,6
-- order by data, id_veiculo, servico, shape_id, timestamp_gps
)
select 
    v.*,
    distancia_teorica,
    distancia_km,
from viagens v
join distancia_estimada d
on v.data = d.data
AND v.id_veiculo = d.id_veiculo
AND v.servico = d.servico
AND v.shape_id = d.shape_id
and v.trip_number = d.trip_number
order by data, id_veiculo, servico, shape_id, datetime_partida 