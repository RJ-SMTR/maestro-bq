SELECT
  data, EXTRACT(HOUR FROM hora_completa) hora, count(distinct id_veiculo) n_veiculos
FROM {{ registros_tratada }}
where fora_garagem is true AND linha IS NOT NULL
GROUP BY data, EXTRACT(HOUR FROM hora_completa)