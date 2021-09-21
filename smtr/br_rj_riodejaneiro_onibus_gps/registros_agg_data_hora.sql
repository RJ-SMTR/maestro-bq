SELECT
  data, EXTRACT(HOUR FROM hora_completa) hora, count(distinct id_veiculo) n_veiculos
FROM {{ gps_sppo }}
where not status_tipo_parada = 'garagem' AND linha IS NOT NULL
GROUP BY data, EXTRACT(HOUR FROM hora_completa)