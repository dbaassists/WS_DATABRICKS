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

-- DBTITLE 1,1 - Inserção de Dados na Camada Silver lendo a Camada Bronze
INSERT INTO adb_treinamento_ws.silver.tb_workshop
SELECT SUBMISSION_TIME
,UPPER(STATUS) STATUS
,UPPER(POSSUI_ALGUM_CONHECIMENTO_EM_AZURE) FLAG_CONHECIMENTO_AZURE
,UPPER(ATUALMENTE_ESTA_TRABALHANDO) FLAG_TRABALHANDO
,UPPER(COMO_FICOU_SABENDO_DO_NOSSO_WORKSHOP) FLAG_COMO_CONHECEU_WORKSHOP
,UPPER(COMO_DESEJA_PAGAR_SUA_INSCRICAO) FLAG_FORMA_PAGAMENTO
,UPPER(ID) ID_INSCRICAO
,CREATED_DATE DATA_CRICAO_REGISTRO
FROM adb_treinamento_ws.bronze.tb_workshop

-- COMMAND ----------

-- DBTITLE 1,2 - Apagando Dados da Camada Bronze
TRUNCATE TABLE adb_treinamento_ws.bronze.tb_workshop
