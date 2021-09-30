SELECT 
  data, EXTRACT(HOUR FROM timestamp_gps) hora, servico as linha, count(distinct id_veiculo) n_veiculos
FROM {{ gps_sppo }}
where not tipo_parada = 'garagem'
GROUP BY data, EXTRACT(HOUR FROM timestamp_gps), servico