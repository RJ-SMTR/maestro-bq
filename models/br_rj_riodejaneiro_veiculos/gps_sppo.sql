{% 
  set partitions_to_replace = [
    'date(current_date("America/Sao_Paulo"))',
    'date(date_sub(current_date("America/Sao_Paulo"), interval 1 day))'
  ]
%}

{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      "field": "data",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["data", "timestamp_gps"],
    partitions = partitions_to_replace,
  )
}}

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
      velocidade,
      linha,
      latitude,
      longitude,

    FROM {{ source('br_rj_riodejaneiro_onibus_gps', 'aux_registros_filtrada') }}
    {% if is_incremental() %}
      WHERE data IN ({{ partitions_to_replace | join(', ') }})
      AND timestamp_gps IN ({{ partitions_to_replace | join(', ') }})
    {% endif %}
  ),
  velocidades AS (
    -- 2. velocidades
    SELECT
      id_veiculo, timestamp_gps, linha, velocidade, distancia, flag_em_movimento
    FROM
      {{ source('br_rj_riodejaneiro_onibus_gps', 'aux_registros_velocidade') }}
    {% if is_incremental() %}
      WHERE data IN ({{ partitions_to_replace | join(', ') }})
      AND timestamp_gps IN ({{ partitions_to_replace | join(', ') }})
    {% endif %}
  ),
  paradas as (
    -- 3. paradas
    SELECT 
      id_veiculo, timestamp_gps, linha, tipo_parada,
    FROM {{ source('br_rj_riodejaneiro_onibus_gps', 'aux_registros_parada') }}
    {% if is_incremental() %}
      WHERE data IN ({{ partitions_to_replace | join(', ') }})
      AND timestamp_gps IN ({{ partitions_to_replace | join(', ') }})
    {% endif %}
  ),
  flags AS (
    -- 4. flag_trajeto_correto
    SELECT
      id_veiculo,
      timestamp_gps, 
      linha,
      route_id, 
      flag_linha_existe_sigmob,
      flag_trajeto_correto, 
      flag_trajeto_correto_hist
    FROM
      {{ source('br_rj_riodejaneiro_onibus_gps', 'aux_registros_flag_trajeto_correto') }}
      {% if is_incremental() %}
        WHERE data IN ({{ partitions_to_replace | join(', ') }})
        AND timestamp_gps IN ({{ partitions_to_replace | join(', ') }})
      {% endif %}
  )
-- 5. Junção final
SELECT
  "SPPO" modo,
  r.timestamp_gps,
  date(r.timestamp_gps) data,
  r.id_veiculo,
  r.linha servico,
  r.latitude,
  r.longitude,
  CASE 
    WHEN 
      flag_em_movimento IS true AND flag_trajeto_correto_hist is true
      THEN true
  ELSE false
  END flag_em_operacao,
  v.flag_em_movimento,
  p.tipo_parada,
  flag_linha_existe_sigmob,
  flag_trajeto_correto,
  flag_trajeto_correto_hist,
  r.velocidade velocidade_instantanea,
  v.velocidade velocidade_estimada_10_min,
  v.distancia,
  STRUCT("lalala" AS versao_maestro, "lelele" AS versao_maestro_bq) versao
FROM
  registros r

JOIN
  flags f
ON
  r.id_veiculo = f.id_veiculo
  AND r.timestamp_gps = f.timestamp_gps
  AND r.linha = f.linha

JOIN
  velocidades v
ON
  r.id_veiculo = v.id_veiculo
  AND  r.timestamp_gps = v.timestamp_gps
  AND  r.linha = v.linha

JOIN 
  paradas p
ON  
  r.id_veiculo = p.id_veiculo
  AND  r.timestamp_gps = p.timestamp_gps
  AND r.linha = p.linha