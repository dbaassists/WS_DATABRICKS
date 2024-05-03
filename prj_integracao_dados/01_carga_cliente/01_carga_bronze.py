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
arquivo = '/mnt/dados/landing_zone/cliente/00_CLIENTE.csv'

# COMMAND ----------

# DBTITLE 1,2 - Importação do Arquivo CSV via Spark

df = (spark.read.options(
    delimiter=';'
    ,header='True'
    ,inferSchema='True'
    ,encoding='utf-8'
    )
    .csv(arquivo)
)


# COMMAND ----------

# DBTITLE 1,3 - Inserção de Dados na Camada Bronze
spark.sql("""
          insert into adb_treinamento_ws.bronze.TB_CLIENTE
          select * 
          ,from_utc_timestamp(current_timestamp(), 'GMT-3') DATA_HORA_CARGA
          from {df}
          """,df=df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC ,row_number() OVER(PARTITION BY CODIGO_CLIENTE ORDER BY DATA_HORA_CARGA DESC) ID
# MAGIC FROM adb_treinamento_ws.bronze.TB_CLIENTE
# MAGIC QUALIFY ID = 1
