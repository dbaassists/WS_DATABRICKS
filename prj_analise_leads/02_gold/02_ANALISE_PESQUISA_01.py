# Databricks notebook source
# MAGIC %md
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

# MAGIC %md
# MAGIC # Etapa de Ingestão do Arquivo de Pesquisas

# COMMAND ----------

# DBTITLE 1,1 - Importação de Biblioteca
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,2 - Importação do Arquivo de Pesquisa
pesquisaArquivo = '/mnt/dados/pesquisa/pesquisa_01.csv'

df = (spark.read.options(
    delimiter=';'
    ,header='True'
    ,inferSchema='True'
    ,encoding='utf-8'
    )
    .csv(pesquisaArquivo)
)

# COMMAND ----------

# DBTITLE 1,3 - Visualização do Arquivo Processado
display(df)

# COMMAND ----------

# DBTITLE 1,4 - Importando o Arquivo Novamente Definindo a sua Estrutura
pesquisaArquivo = '/mnt/dados/pesquisa/pesquisa_01.csv'

pesquisaSchema = StructType([
    StructField("DATA_INSCRICAO", TimestampType(), True),
    StructField("AREA_ATUACAO", StringType(), True),
    StructField("FORMA_INGRESSO_AREA_DADOS", StringType(), True),
    StructField("TRABALHA_COM_CLOUD", StringType(), True),
    StructField("LINGUAGEM_PROGRAMACAO", StringType(), True),
    StructField("FERRAMENTA_INTEGRACAO", StringType(), True),
    StructField("SGBD", StringType(), True),
    StructField("CI_CD", StringType(), True),
    StructField("FLAG_AZURE_KEY_VAULT", StringType(), True),
    StructField("FLAG_STORAGE_ACCOUNT", StringType(), True),
    StructField("FLAG_DATABRICKS", StringType(), True),
    StructField("ID_INSCRICAO", StringType(), True),
    StructField("OWNER", StringType(), True),
    StructField("DATA_CRICAO_REGISTRO", TimestampType(), True),
    StructField("DATA_ALTERACAO_REGISTRO", TimestampType(), True)
])

df = (spark.read.options(
    delimiter=';'
    ,header='True'
    ,inferSchema='True'
    ,encoding='utf-8'
    ).schema(pesquisaSchema)
    .csv(pesquisaArquivo)
)

# COMMAND ----------

# DBTITLE 1,5 - Visualizando a Estrtura do Arquivo
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Iniciando a Etapa de Análise dos Dados
# MAGIC

# COMMAND ----------

display(spark.sql("""
                  SELECT AREA_ATUACAO, COUNT(AREA_ATUACAO) QTD
                  FROM {df}
                  GROUP BY AREA_ATUACAO
                  ORDER BY 2 DESC
                  """, df=df))                  

# COMMAND ----------

display(spark.sql("""
                  SELECT FORMA_INGRESSO_AREA_DADOS, COUNT(AREA_ATUACAO) QTD
                  FROM {df}
                  GROUP BY FORMA_INGRESSO_AREA_DADOS
                  ORDER BY 2 DESC
                  """, df=df))

# COMMAND ----------

display(spark.sql("""
                  SELECT TRABALHA_COM_CLOUD, COUNT(AREA_ATUACAO) QTD
                  FROM {df}
                  GROUP BY TRABALHA_COM_CLOUD
                  ORDER BY 2 DESC
                  """, df=df))                  

# COMMAND ----------

display(spark.sql("""
                  SELECT FLAG_AZURE_KEY_VAULT, COUNT(AREA_ATUACAO) QTD
                  FROM {df}
                  GROUP BY FLAG_AZURE_KEY_VAULT
                  ORDER BY 2 DESC
                  """, df=df))                  

# COMMAND ----------

display(spark.sql("""
                  SELECT CI_CD, COUNT(AREA_ATUACAO) QTD
                  FROM {df}
                  GROUP BY CI_CD
                  ORDER BY 2 DESC
                  """, df=df))                  

# COMMAND ----------

display(spark.sql("""
                  SELECT FLAG_STORAGE_ACCOUNT, COUNT(AREA_ATUACAO) QTD
                  FROM {df}
                  GROUP BY FLAG_STORAGE_ACCOUNT
                  ORDER BY 2 DESC
                  """, df=df))                  

# COMMAND ----------

display(spark.sql("""
                  SELECT FLAG_DATABRICKS, COUNT(AREA_ATUACAO) QTD
                  FROM {df}
                  GROUP BY FLAG_DATABRICKS
                  ORDER BY 2 DESC
                  """, df=df))                  

# COMMAND ----------

display(spark.sql("""
                  SELECT FERRAMENTA_INTEGRACAO, SUM(QTD) QTD
                  FROM
                  (
                    SELECT SPLIT(FERRAMENTA_INTEGRACAO,',')[0] FERRAMENTA_INTEGRACAO , 1 QTD
                    FROM {df}
                    UNION ALL
                    SELECT SPLIT(FERRAMENTA_INTEGRACAO,',')[1] FERRAMENTA_INTEGRACAO , 1 QTD
                    FROM {df}
                    UNION ALL
                    SELECT SPLIT(FERRAMENTA_INTEGRACAO,',')[2] FERRAMENTA_INTEGRACAO , 1 QTD
                    FROM {df}
                    UNION ALL
                    SELECT SPLIT(FERRAMENTA_INTEGRACAO,',')[3] FERRAMENTA_INTEGRACAO , 1 QTD
                    FROM {df}
                    UNION ALL
                    SELECT SPLIT(FERRAMENTA_INTEGRACAO,',')[4] FERRAMENTA_INTEGRACAO , 1 QTD
                    FROM {df}                    
                    )
                    WHERE FERRAMENTA_INTEGRACAO IS NOT NULL
                    GROUP BY FERRAMENTA_INTEGRACAO
                    ORDER BY 2 DESC
                  """, df=df))                  

# COMMAND ----------

display(spark.sql("""
                  SELECT SGBD, SUM(QTD) QTD
                  FROM
                  (
                    SELECT SPLIT(SGBD,',')[0] SGBD , 1 QTD
                    FROM {df}
                    UNION ALL
                    SELECT SPLIT(SGBD,',')[1] SGBD , 1 QTD
                    FROM {df}
                    UNION ALL
                    SELECT SPLIT(SGBD,',')[2] SGBD , 1 QTD
                    FROM {df}
                    UNION ALL
                    SELECT SPLIT(SGBD,',')[3] SGBD , 1 QTD
                    FROM {df}                    
                    )
                    WHERE SGBD IS NOT NULL
                    GROUP BY SGBD
                    ORDER BY 2 DESC
                  """, df=df))


