# Databricks notebook source
# MAGIC %md
# MAGIC Regras, Descrições (macro) e detalhes
# MAGIC
# MAGIC |N° Controle|Descrição|Dados
# MAGIC |---|---|---|
# MAGIC |01|Camada| Bronze - Silver - Gold |
# MAGIC |02|Data Criação| Quando o Fluxo foi Criado |
# MAGIC |03|Desenvolvida por| Quem desenvolver? |
# MAGIC |04|Objetivo| Por que foi criado? |
# MAGIC |05|Solicitante| Quem é o demandante? |
# MAGIC |06|Projeto| Atende algum projeto? |
# MAGIC |07|Nome do Pipeline| Esse notebool é usado em alguma Pipeline do Azure Data Factory? |
# MAGIC |08|Data Alteração| Foi alterado depois de criado? ||

# COMMAND ----------

# DBTITLE 1,1 - Criação de Variável
pesquisaArquivo = '/mnt/dados/landing_zone/workshop/inscritos_workshop.csv'

# COMMAND ----------

# DBTITLE 1,2 - Importação do Arquivo CSV via Spark

df = (spark.read.options(
    delimiter=';'
    ,header='True'
    ,inferSchema='True'
    ,encoding='utf-8'
    )
    .csv(pesquisaArquivo)
)

# COMMAND ----------

# DBTITLE 1,3 - Inserção de Dados na Camada Bronze
spark.sql("""
          insert into adb_treinamento_ws.bronze.tb_workshop
          select * from {df}
          """,df=df)
