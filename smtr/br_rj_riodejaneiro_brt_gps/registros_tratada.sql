/*
Descrição:
Junção dos passos de tratamento, junta as informações extras que definimos a partir dos registros
capturados.
Para descrição detalhada de como cada coluna é calculada, consulte a documentação de cada uma das tabelas
utilizadas abaixo.
1. registros_filtrada: filtragem e tratamento básico dos dados brutos capturados.
2. aux_registros_velocidade: estimativa da velocidade de veículo a cada ponto registrado e identificação
do estado de movimento ('parado', 'andando')
3. aux_registros_parada: identifica veículos parados em terminais ou garagens conhecidas
4. aux_registros_flag_trajeto_correto: calcula intersecções das posições registradas para cada veículo
com o traçado da linha informada.
5. As junções (joins) são feitas sobre o id_veículo e a timestamp_gps.
*/
WITH
  registros as (
  -- 1. registros_filtrada
    SELECT 
      id_veiculo,
      timestamp_gps,
      timestamp_captura,
      linha,
      latitude,
      longitude,

    FROM {{ registros_filtrada }}
  )
  velocidades AS (
    -- 2. velocidades
    SELECT
      id_veiculo, timestamp_gps, velocidade, status_movimento
    FROM {{ velocidade }} 
    WHERE data BETWEEN DATE({{ date_range_start }}) AND DATE({{ date_range_end }})
  ),
  paradas as (
    -- 3. paradas
    SELECT 
      id_veiculo, timestamp_gps, status_tipo_parada,
    FROM {{ parada }}
    WHERE data BETWEEN DATE({{ date_range_start }}) AND DATE({{ date_range_end }})
  ),
  flags AS (
    -- 4. flag_trajeto_correto
    SELECT
      id_veiculo, timestamp_gps, flag_linha_existe_sigmob,flag_trajeto_correto, flag_trajeto_correto_hist
    FROM {{ flag_trajeto_correto }}
    WHERE data BETWEEN DATE({{ date_range_start }}) AND DATE({{ date_range_end }})  
  )
-- 5. Junção final
SELECT
  date(r.timestamp_gps) data,
  r.timestamp_gps,
  extract(time FROM r.timestamp_gps) AS hora_completa, 
  r.id_veiculo,
  r.latitude,
  r.longitude,
  v.velocidade velocidade_estimada_10_min,
  v.status_movimento,
  status_tipo_parada,
  r.linha,
  flag_linha_existe_sigmob,
  flag_trajeto_correto,
  flag_trajeto_correto_hist,
  STRUCT({{ maestro_sha }} AS versao_maestro, {{ maestro_bq_sha }} AS versao_maestro_bq) versao
FROM
  registros r

JOIN
  velocidades v
ON
  r.id_veiculo = v.id_veiculo
  AND  r.timestamp_gps = v.timestamp_gps

JOIN
  flags f
ON
  r.id_veiculo = f.id_veiculo
  AND r.timestamp_gps = f.timestamp_gps
JOIN 
  paradas p
ON  
  r.id_veiculo = p.id_veiculo
  AND  r.timestamp_gps = p.timestamp_gps

