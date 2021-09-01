create or replace table `rj-smtr-dev.projeto_multa_automatica.onibus_registros_tratada_1_dia` as
select 
*
from rj-smtr.br_rj_riodejaneiro_onibus_gps.registros_tratada
where data between '2021-07-10' and '2021-07-19'