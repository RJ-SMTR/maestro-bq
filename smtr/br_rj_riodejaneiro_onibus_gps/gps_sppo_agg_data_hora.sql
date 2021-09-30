SELECT
  data, EXTRACT(HOUR FROM timestamp_gps) hora, count(distinct id_veiculo) n_veiculos
FROM {{ gps_sppo }}
where not tipo_parada = 'garagem' AND servico IS NOT NULL
GROUP BY data, EXTRACT(HOUR FROM timestamp_gps)