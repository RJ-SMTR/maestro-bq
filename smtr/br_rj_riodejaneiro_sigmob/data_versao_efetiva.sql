with
agency as (
SELECT 
    data,
    data_versao as data_versao_original, 
    CASE WHEN data < DATE("{{ data_inclusao_agency }}") THEN DATE("{{ data_inclusao_agency }}") ELSE
        LAST_VALUE(DATE(data_versao) IGNORE NULLS) OVER (ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS data_versao_efetiva
FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', CURRENT_DATE())) data
LEFT JOIN (SELECT DISTINCT data_versao
    FROM {{ agency }}
    WHERE data_versao between date({{ date_range_start }}) and date({{ date_range_end }}))
ON data = DATE(data_versao)
),
calendar as (
SELECT 
    data,
    data_versao as data_versao_original, 
    CASE WHEN data <= DATE("{{ data_inclusao_calendar }}") THEN DATE("{{ data_inclusao_calendar }}") ELSE
        LAST_VALUE(DATE(data_versao) IGNORE NULLS) OVER (ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS data_versao_efetiva
FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', CURRENT_DATE())) data
LEFT JOIN (SELECT DISTINCT data_versao
    FROM {{ calendar }}
    WHERE data_versao between date({{ date_range_start }}) and date({{ date_range_end }}))
ON data = DATE(data_versao)
),
frota_determinada as (
SELECT 
    data,
    DATE(data_versao) as data_versao_original, 
    CASE WHEN data <= DATE("{{ data_inclusao_frota_determinada }}") THEN DATE("{{ data_inclusao_frota_determinada }}") ELSE
        LAST_VALUE(DATE(data_versao) IGNORE NULLS) OVER (ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS data_versao_efetiva
FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', CURRENT_DATE())) data
LEFT JOIN (SELECT DISTINCT data_versao
    FROM {{ frota_determinada }}
    WHERE data_versao between date({{ date_range_start }}) and date({{ date_range_end }}))
ON DATE(data) = DATE(data_versao)
),
linhas as (
    SELECT 
    data,
    DATE(data_versao) as data_versao_original, 
    CASE WHEN data < DATE("{{ data_inclusao_linhas }}") THEN DATE("{{ data_inclusao_linhas }}") ELSE
        LAST_VALUE(DATE(data_versao) IGNORE NULLS) OVER (ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS data_versao_efetiva
FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', CURRENT_DATE())) data
LEFT JOIN (SELECT DISTINCT data_versao
    FROM {{ linhas }}
    WHERE data_versao between date({{ date_range_start }}) and date({{ date_range_end }}))
ON data = DATE(data_versao)
),
routes as (
SELECT 
    data,
    DATE(data_versao) as data_versao_original, 
    CASE WHEN data < DATE("{{ data_inclusao_routes }}") THEN DATE("{{ data_inclusao_routes }}") ELSE
        LAST_VALUE(DATE(data_versao) IGNORE NULLS) OVER (ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS data_versao_efetiva
FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', CURRENT_DATE())) data
LEFT JOIN (SELECT DISTINCT data_versao
    FROM {{ routes }}
    WHERE data_versao between date({{ date_range_start }}) and date({{ date_range_end }}))
ON data = data_versao
),
shapes as (
SELECT 
    data,
    data_versao as data_versao_original, 
   CASE WHEN data < DATE("{{ data_inclusao_shapes }}") THEN DATE("{{ data_inclusao_shapes }}") ELSE
        LAST_VALUE(DATE(data_versao) IGNORE NULLS) OVER (ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS data_versao_efetiva
FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', CURRENT_DATE())) data
LEFT JOIN (SELECT DISTINCT data_versao
    FROM {{ shapes_geom }}
    WHERE data_versao between date({{ date_range_start }}) and date({{ date_range_end }}))
ON data = DATE(data_versao)
),
stop_details as (
SELECT 
    data,
    data_versao as data_versao_original, 
    CASE WHEN data <= DATE("{{ data_inclusao_stop_details }}") THEN DATE("{{ data_inclusao_stop_details }}") ELSE
        LAST_VALUE(DATE(data_versao) IGNORE NULLS) OVER (ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS data_versao_efetiva
FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', CURRENT_DATE())) data
LEFT JOIN (SELECT DISTINCT data_versao
    FROM {{ stop_details }}
    WHERE data_versao between date({{ date_range_start }}) and date({{ date_range_end }}))
ON data = DATE(data_versao)
),
stop_times as (
SELECT 
    data,
    data_versao as data_versao_original, 
    CASE WHEN data < DATE("{{ data_inclusao_stop_times }}") THEN DATE("{{ data_inclusao_stop_times }}") ELSE
        LAST_VALUE(DATE(data_versao) IGNORE NULLS) OVER (ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS data_versao_efetiva
FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', CURRENT_DATE())) data
LEFT JOIN (SELECT DISTINCT data_versao
    FROM {{ stop_times }}
    WHERE data_versao between date({{ date_range_start }}) and date({{ date_range_end }}))
ON data = DATE(data_versao)
),
stops as (
SELECT 
    data,
    data_versao as data_versao_original, 
    CASE WHEN data < DATE("{{ data_inclusao_stops }}") THEN DATE("{{ data_inclusao_stops }}") ELSE
        LAST_VALUE(DATE(data_versao) IGNORE NULLS) OVER (ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS data_versao_efetiva
FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', CURRENT_DATE())) data
LEFT JOIN (SELECT DISTINCT data_versao
    FROM {{ stops }}
    WHERE data_versao between date({{ date_range_start }}) and date({{ date_range_end }}))
ON data = DATE(data_versao)
),
trips as (
SELECT 
    data,
    data_versao as data_versao_original, 
    CASE WHEN data < DATE"{{ data_inclusao_trips }}" THEN DATE("{{ data_inclusao_trips }}") ELSE
        LAST_VALUE(DATE(data_versao) IGNORE NULLS) OVER (ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS data_versao_efetiva
FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', CURRENT_DATE())) data
LEFT JOIN (SELECT DISTINCT data_versao
    FROM {{ trips }}
    WHERE data_versao between date({{ date_range_start }}) and date({{ date_range_end }}))
ON data = DATE(data_versao)
)
select
s.data,
a.data_versao_efetiva as data_versao_efetiva_agency,
c.data_versao_efetiva as data_versao_efetiva_calendar,
f.data_versao_efetiva as data_versao_efetiva_frota_determinada,
l.data_versao_efetiva as data_versao_efetiva_linhas,
r.data_versao_efetiva as data_versao_efetiva_routes,
s.data_versao_efetiva as data_versao_efetiva_shapes,
sd.data_versao_efetiva as data_versao_efetiva_stop_details,
st.data_versao_efetiva as data_versao_efetiva_stop_times,
sp.data_versao_efetiva as data_versao_efetiva_stops,
t.data_versao_efetiva as data_versao_efetiva_trips
from shapes s 
join agency a
on s.data = a.data
join calendar c
on s.data = c.data
join frota_determinada f
on s.data = f.data
join linhas l
on s.data = l.data
join routes r
on s.data = r.data
join stops sp
on s.data = sp.data
join stop_details sd
on s.data = sd.data
join stop_times st
on s.data = st.data
join trips t
on s.data = t.data