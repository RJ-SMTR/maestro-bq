SELECT *
FROM `rj-smtr.br_rj_riodejaneiro_onibus_gps.registros_tratada`
WHERE data between DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) and CURRENT_DATE()