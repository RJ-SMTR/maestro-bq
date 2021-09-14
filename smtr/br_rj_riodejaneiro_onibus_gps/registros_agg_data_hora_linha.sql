SELECT 
  data, EXTRACT(HOUR FROM hora_completa) hora, linha, count(distinct id) n_veiculos
FROM {{ registros_tratada }}
where fora_garagem is true
GROUP BY data, EXTRACT(HOUR FROM hora_completa), linha