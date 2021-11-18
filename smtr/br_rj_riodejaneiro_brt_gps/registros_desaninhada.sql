SELECT 
data,
hora,
id_veiculo,
timestamp_gps,
timestamp_captura,
json_value(content,"$.latitude") latitude,
json_value(content,"$.longitude") longitude,
json_value(content,"$.servico") servico,
json_value(content,"$.sentido") sentido,
json_value(content,"$.velocidade") velocidade,
from rj-smtr.br_rj_riodejaneiro_brt_gps.registros as t