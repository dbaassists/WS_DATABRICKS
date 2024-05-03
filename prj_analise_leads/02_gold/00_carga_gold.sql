-- Databricks notebook source
-- MAGIC %md
-- MAGIC Regras, Descrições (macro) e detalhes
-- MAGIC
-- MAGIC |N° Controle|Descrição|Dados
-- MAGIC |---|---|---|
-- MAGIC |01|Camada| Bronze - Silver - Gold |
-- MAGIC |02|Data Criação| Quando o Fluxo foi Criado |
-- MAGIC |03|Desenvolvida por| Quem desenvolver? |
-- MAGIC |04|Objetivo| Por que foi criado? |
-- MAGIC |05|Solicitante| Quem é o demandante? |
-- MAGIC |06|Projeto| Atende algum projeto? |
-- MAGIC |07|Nome do Pipeline| Esse notebool é usado em alguma Pipeline do Azure Data Factory? |
-- MAGIC |08|Data Alteração| Foi alterado depois de criado? ||

-- COMMAND ----------

-- DBTITLE 1,1 - Inserção de Dados na Camada Gold lendo a Camada Silver
TRUNCATE TABLE  adb_treinamento_ws.gold.tb_workshop;
INSERT INTO adb_treinamento_ws.gold.tb_workshop
SELECT * 
FROM adb_treinamento_ws.silver.tb_workshop

-- COMMAND ----------

-- DBTITLE 1,2 - Apagando os Dados da Camada Silver
TRUNCATE TABLE adb_treinamento_ws.silver.tb_workshop

-- COMMAND ----------

-- DBTITLE 1,3 - Aplicando Optimize em uma Tabela
optimize adb_treinamento_ws.gold.tb_workshop;
