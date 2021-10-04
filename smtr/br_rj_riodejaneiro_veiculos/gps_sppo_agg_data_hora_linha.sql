SELECT 
  data, EXTRACT(HOUR FROM timestamp_gps) hora, servico as linha, count(distinct id_veiculo) n_veiculos
FROM {{ gps_sppo }}
where not tipo_parada = 'garagem'
and data between DATE({{ date_range_start }}) and DATE({{ date_range_end }})
GROUP BY data, EXTRACT(HOUR FROM timestamp_gps), servico