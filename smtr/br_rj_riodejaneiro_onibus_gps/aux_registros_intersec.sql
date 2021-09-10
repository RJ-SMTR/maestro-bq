WITH shapes AS
( /* Buffer shapes to compensate gps precision, buffer boundary points to detect start/end of trip */
	SELECT  *
	FROM {{ shapes }}
), registros AS
( /* Generate ponto_carro for GEOG operations */
	SELECT  *
	       ,ST_GEOGPOINT(longitude,latitude) ponto_carro
	FROM {{ registros_filtrada }}
), times AS
( /* Generate empty TABLE of intervals */
	SELECT  faixa_horaria
	FROM
	(
		SELECT  CAST(MIN(data)               AS TIMESTAMP) min_date
		       ,TIMESTAMP_ADD(CAST(MAX(data) AS TIMESTAMP),INTERVAL 1 day) max_date
		FROM registros
	) r
	-- this is a line
	JOIN UNNEST
	(GENERATE_TIMESTAMP_ARRAY(r.min_date, r.max_date, INTERVAL {{ faixa_horaria_minutos }} minute)
	) faixa_horaria
), faixas AS
( /*
	JOIN registros WITH intervals generated above */
	SELECT  id_veiculo
	       ,linha
	       ,timestamp_captura
	       ,faixa_horaria
	       ,longitude
	       ,latitude
	       ,ponto_carro
	       ,data
	       ,hora
	FROM times t
	JOIN registros r
	ON (r.timestamp_captura BETWEEN datetime(faixa_horaria) AND datetime(timestamp_add(faixa_horaria, interval {{ faixa_horaria_minutos }} minute)))
), intersects AS
( /* Count number of intersects BETWEEN vehicle AND informed route shape */
	SELECT  id_veiculo
	       ,f.linha                                                                                                                   AS linha_gps
	       ,s.linha_gtfs
	       ,shape_distance                                                                                                            AS distancia
	       ,data
	       ,hora
	       ,faixa_horaria
	       ,s.shape_id                                                                                                                AS trip_id
	       ,MIN(timestamp_captura)                                                                                                    AS timestamp_inicio
	       ,COUNT(timestamp_captura)                                                                                                  AS total_capturas
	       ,COUNT(case WHEN st_dwithin(ponto_carro,shape,{{ tamanho_buffer_metros}}) THEN 1 end) n_intersec
	       ,CASE WHEN COUNT(case
	             WHEN st_dwithin(start_pt,ponto_carro,{{ tamanho_buffer_metros }}) is true THEN 1 END)>=1 THEN 'start'
	             WHEN COUNT(case
	             WHEN st_dwithin(end_pt,ponto_carro,{{ tamanho_buffer_metros }}) is true THEN 1 END)>=1 THEN 'end'  ELSE 'middle' END AS status
	FROM faixas f
	JOIN shapes s
	ON s.data_versao = f.data AND f.linha = linha_gtfs
	GROUP BY  id_veiculo
	         ,faixa_horaria
	         ,linha_gps
	         ,linha_gtfs
	         ,trip_id
	         ,data
	         ,hora
	         ,distancia
)
SELECT  *
       ,STRUCT({{ maestro_sha }} AS versao_maestro,{{ maestro_bq_sha }} AS versao_maestro_bq) versao
FROM intersects
WHERE n_intersec>0 