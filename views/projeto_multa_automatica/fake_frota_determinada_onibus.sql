create or replace table `rj-smtr-dev.projeto_multa_automatica.fake_frota_determinada_onibus` as
select 
    linha, fhoffa.x.random_int(3, 15) frota_determinada
from (
    select distinct linha
    from `rj-smtr-dev.projeto_multa_automatica.onibus_registros_tratada_sample`)