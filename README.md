# maestro-bq
Versionamento de queries para o BigQuery, herdadas do repositório https://github.com/RJ-SMTR/maestro


## Testando a query

Crie um virtualenv e instale as dependências:

```
conda create -n maestro-bq
conda activate maestro-bq
pip install -r requirements.txt
```

Renderize uma query:

`python helper/helper.py render <query-path>`

Por exemplo:

`python helper/helper.py render smtr/brt_rj_riodejaneiro_sigmob/data_versao_efetiva`

A query renderizada será printada na tela e também será salva no seu clipboard.
Para testar se ela compila, basta executá-la no BigQuery.

Você pode mudar os atributos descritos na próxima secão no arquivo `helper/inner_params.yaml`.

## Atributos disponíveis para queries

- `run_key`: a chave montada no Dagster para essa execução
- `run_timestamp`: estampa de tempo dessa execução (exclusivo para cada execução de pipeline)
- `maestro_sha`: hash do commit mais recente na branch default do repositório maestro
- `maestro_bq_sha`: hash do commit mais recente na branch default do repositório maestro-bq
- `date_range_start`: início da janela de filtragem para backfill
  - Coincide com o `backfill.start_timestamp` do YAML na criação da tabela
  - Coincide com o timestamp da última execução nas outras execuções
- `date_range_end`: fim da janela de filtragem para backfill
  - Coincide com o timestamp do sensor (único para todas as RunRequests daquela execução do sensor)
- Tudo que estiver dentro da chave `parameters` do YAML, sem o prefixo `parameters.*`
  - Exemplo: no YAML -> `parameters.interval = 2`, na query -> `{{ interval }}`

## Exemplo de materialized view

- Arquivo de query em `maestro-bq/materialized_views/br_rj_riodejaneiro_brt_gps/materialized_registros_tratada.sql`:
```sql
WITH box AS (
  SELECT
    *
  FROM `rj-smtr.br_rj_riodejaneiro_geo.limites_geograficos_caixa`
)
SELECT
  *,
  {{ run_key }} AS run_key,
  {{ run_timestamp }} AS run_timestamp,
  {{ maestro_sha }} AS maestro_sha,
  {{ maestro_bq_sha }} AS maestro_bq_sha,
FROM `rj-smtr.br_rj_riodejaneiro_brt_gps.registros` 
WHERE DATETIME_DIFF(timestamp_captura, timestamp_gps, MINUTE) < {{ interval }}
AND timestamp_gps >= {{ date_range_start }}
AND timestamp_gps < {{ date_range_end }}
AND longitude BETWEEN (SELECT min_longitude FROM box) AND (SELECT max_longitude FROM box)
AND latitude BETWEEN (SELECT min_latitude FROM box) AND (SELECT max_latitude FROM box)
```

- Arquivo de configuração em `maestro-bq/materialized_views/br_rj_riodejaneiro_brt_gps/materialized_registros_tratada.yaml`:
```yaml
# Definição de taxa de atualização
scheduling:
  cron: "0/5 * * * *"

# Definição de particionamento
# - column: nome da coluna
# - type: deve ser um dos seguintes
#   * DATE (não exige período)
#   * DATE_TRUNC (exige período MONTH/YEAR)
#   * DATETIME_TRUNC (exige período DAY/HOUR/MONTH/YEAR)
#   * TIMESTAMP_TRUNC (exige período DAY/HOUR/MONTH/YEAR)
#   * <To-do> RANGE_BUCKET
#   * <To-do> GENERATE_ARRAY
# - period: período (conforme especificado acima)
partitioning:
  column: "timestamp_gps"
  type: "DATE"
  period: ""

# Parâmetros da query
parameters:
  interval: 2

# Parâmetros de backfill
backfill:
  # Formato %Y-%m-%d %H:%M:%S
  start_timestamp: "2021-01-01 00:00:00"
```

- Resultado: criação da tabela `br_rj_riodejaneiro_brt_gps.materialized_registros_tratada` que se materializa incrementalmente a cada 5 minutos e particionada diariamente em `timestamp_gps`
