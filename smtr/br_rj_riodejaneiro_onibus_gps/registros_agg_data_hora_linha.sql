SELECT 
  data, EXTRACT(HOUR FROM hora_completa) hora, linha, count(distinct id) n_veiculos
FROM {{ registros_tratada }}
where not status_tipo_parada = 'garagem'
GROUP BY data, EXTRACT(HOUR FROM hora_completa), linha