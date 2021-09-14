WITH
registros as (
	SELECT *
	FROM {{	 registros_filtrada }}
),
times AS ( /* Generate empty TABLE of intervals */
	SELECT
		faixa_horaria
	FROM (
		SELECT
			CAST(MIN(data) AS TIMESTAMP) min_date,
			TIMESTAMP_ADD(CAST(MAX(data) AS TIMESTAMP),INTERVAL 1 day) max_date
		FROM registros) r
	JOIN
		UNNEST (GENERATE_TIMESTAMP_ARRAY(r.min_date,
			r.max_date,
		INTERVAL {{ faixa_horaria_minutos }} minute) ) faixa_horaria ),
faixas AS ( /*
	JOIN registros WITH intervals generated above */
	SELECT
		id_veiculo,
		linha,
		timestamp_captura,
		faixa_horaria,
		longitude,
		latitude,
		posicao_veiculo_geo,
		DATA,
		hora
	FROM
		times t
	JOIN
		registros r
	ON
		(r.timestamp_captura BETWEEN datetime(faixa_horaria)
		AND datetime(TIMESTAMP_ADD(faixa_horaria, INTERVAL {{ faixa_horaria_minutos }} minute))) ),
intersects AS ( /* Count number of intersects BETWEEN vehicle AND informed route shape */
	SELECT
		id_veiculo,
		f.linha AS linha_gps,
		s.linha_gtfs,
		shape_distance AS distancia,
		data,
		hora,
		faixa_horaria,
		s.shape_id AS trip_id,
		MIN(timestamp_captura) AS timestamp_inicio,
		COUNT(timestamp_captura) AS total_capturas,
		COUNT(CASE
			WHEN st_dwithin(posicao_veiculo_geo, shape, {{ tamanho_buffer_metros}}) THEN 1
		END
		) n_intersec,
		CASE
		WHEN COUNT(CASE
			WHEN st_dwithin(start_pt,
			posicao_veiculo_geo,
			{{ tamanho_buffer_metros }}) IS TRUE THEN 1
		END
		)>=1 THEN 'start'
		WHEN COUNT(CASE
			WHEN st_dwithin(end_pt,
			posicao_veiculo_geo,
			{{ tamanho_buffer_metros }}) IS TRUE THEN 1
		END
		)>=1 THEN 'end'
		ELSE
		'middle'
	END
		AS status
	FROM (
		SELECT t1.*, t2.data_versao_efetiva
		FROM faixas t1
		JOIN  {{ data_versao_efetiva }} t2
		ON t1.data = t2.data) f
	JOIN
		{{ shapes }} s
	ON
		s.data_versao = f.data_versao_efetiva
		AND f.linha = s.linha_gtfs
	GROUP BY
		id_veiculo,
		faixa_horaria,
		linha_gps,
		linha_gtfs,
		trip_id,
		data,
		hora,
		distancia )
SELECT
  *,
  STRUCT({{ maestro_sha }} AS versao_maestro,
    {{ maestro_bq_sha }} AS versao_maestro_bq) versao
FROM
  intersects
WHERE
  n_intersec > 0