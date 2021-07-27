WITH paradas as (
  /*
  paradas:
    Tabela contendo os dados geográficos (lat/lon) para todas as estações e garagens que temos cadastradas para o BRT, cujas tabelas base são as presentes 
    nas cláusulas FROM.
    A tabela em si é apenas uma união de todas as garagens e estações que estejam cadastradas como ativas (cláusula 'where ativa = 1')
  */
  select
    ST_GEOGPOINT(longitude, latitude) ponto_parada, nome_estacao nome_parada, 'estacao' tipo_parada
  from `rj-smtr.br_rj_riodejaneiro_transporte.estacoes_e_terminais_brt` t1
  union all
  select
    ST_GEOGPOINT(longitude, latitude) ponto_parada, nome_empresa nome_parada, 'garagem' tipo_parada
  from `rj-smtr.br_rj_riodejaneiro_transporte.garagens` t2
  where ativa = 1),
onibus_parados AS (
  /*
  onibus_parados:
    Neste passo, onibus parados é uma tabela que somente apresenta os dados para cada carro e suas respectivas velocidades médias, calculadas sobre faixas
    horárias de 10 minutos e com o novo campo 'ponto_carro', criado a partir de ST_GEOGPOINT() por conveniência para o cálculo das distâncias entre pontos
    geográficos.
  */
  select
    *, ST_GEOGPOINT(longitude, latitude) ponto_carro
  from `rj-smtr.dashboard_monitoramento_brt.velocidade_carro` 
  ),
distancia AS (
  /*
  distancia:
    Nesta tabela selecionamos os campos com dados relativos a cada carro e calculamos sua distância com relação à parada mais próxima e indexamos os 
    resultados dentro de cada partição (cláusula PARTITION BY) ordenando pela distância da parada e salvamos o índice em 'n_row'.
    A ordenação dos indíces em ordem crescente de distância da parada, nos permitirá selecionar somente a menor distância (relativa à parada mais próxima) no
    próximo passo. Além disso, a junção da tabela paradas com onibus_parados se dá onde 1=1 (?????????????)
  */
  SELECT 
    timestamp_captura, velocidade, placa, longitude, latitude, nome_parada, tipo_parada,
    ST_DISTANCE(ponto_carro, ponto_parada) distancia_parada, 
    ROW_NUMBER() OVER (PARTITION BY timestamp_captura, placa ORDER BY ST_DISTANCE(ponto_carro, ponto_parada)) nrow
  FROM paradas p
  join onibus_parados o
  on 1=1
  )
  /*
  Por fim, selecionamos todos os campos exceto 'n_row' de distancia e criamos os novos campos 'status_movimento' e 'status_tipo_parada'.
    status_movimento:
      Campo criado condicionalmente (cláusula CASE) que discrimina os carros em duas categorias, "parado" quando a velocidade na faixa horária avaliada é
      menor que 3 km/h e "andando", caso contrário.
    status_tipo_parada:
      Outro campo criado condicionalmente que discrimina as paradas consideradas de acordo com a distância do carro em questão à parada sendo considerada.
      Caso o carro esteja a menos de 1000m da parada, consideramos o tipo da parada cadastrado em 'tipo_parada' de paradas, caso contrário, consideramos a 
      parada como "não identificado"
  O filtro aplicado ao fim (clausula WHERE) faz com que peguemos apenas a menor distância do carro à uma parada, dado que, em distancia, calculamos a 
    distância do carro à todas as paradas, usamos 'n_row = 1' pois a coluna está em ordenação crescente das distâncias até as paradas.
  */
SELECT
  * except(nrow),
  case
    when velocidade < 3 then 'parado'
    else 'andando'
  end status_movimento,
  case
    when distancia_parada < 1000 then tipo_parada
    else 'nao_identificado'
  end status_tipo_parada
FROM distancia
WHERE nrow = 1