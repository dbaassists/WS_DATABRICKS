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

-- DBTITLE 1,01 - Criação da Tabela "tb_workshop" na camada Bronze
drop table if exists adb_treinamento_ws.bronze.tb_workshop;
create table if not exists adb_treinamento_ws.bronze.tb_workshop
(
SUBMISSION_TIME STRING COMMENT 'Data da Criação do Lead'
,STATUS STRING COMMENT 'Status do Lead'
,POSSUI_ALGUM_CONHECIMENTO_EM_AZURE STRING COMMENT 'Flag Conhecimento em Azure'
,ATUALMENTE_ESTÁ_TRABALHANDO STRING COMMENT 'Flag Trabalha, SIM ou NÃO'
,COMO_FICOU_SABENDO_DO_NOSSO_WORKSHOP STRING COMMENT 'Flag Como Conheceu o Workshop'
,COMO_DESEJA_PAGAR_SUA_INSCRIÇÃO STRING COMMENT 'Id gerado automático'
,ID STRING COMMENT 'Id gerado automático'
,CREATED_DATE STRING COMMENT 'Data de criação do registro'
)
using delta
location '/mnt/dados/bronze/tb_workshop/'
comment 'Tabela de dados do produto WORKSHOP na camada BRONZE'

-- COMMAND ----------

-- DBTITLE 1,02 - Criação da Tabela "tb_workshop" na camada Silver
drop table if exists adb_treinamento_ws.silver.tb_workshop;
create table if not exists adb_treinamento_ws.silver.tb_workshop
(
DATA_INSCRICAO  TIMESTAMP COMMENT 'Data da Criação do Lead'
,STATUS  STRING COMMENT 'Status do Lead'
,FLAG_CONHECIMENTO_AZURE STRING COMMENT 'Flag Conhecimento em Azure'
,FLAG_TRABALHANDO STRING COMMENT 'Flag Trabalha, SIM ou NÃO'
,FLAG_COMO_CONHECEU_WORKSHOP STRING COMMENT 'Flag Como Conheceu o Workshop'
,FLAG_FORMA_PAGAMENTO STRING COMMENT 'Flag Forma de Pagamento'
,ID_INSCRICAO STRING COMMENT 'Id gerado automático'
,DATA_CRICAO_REGISTRO TIMESTAMP COMMENT 'Data de criação do registro'
)
using delta
--location '/mnt/dados/tb_workshop/'
comment 'Tabela de dados do produto WORKSHOP na camada SILVER'

-- COMMAND ----------

-- DBTITLE 1,03 - Criação da Tabela "tb_workshop" na camada Gold
drop table if exists adb_treinamento_ws.golg.tb_workshop;
create table if not exists adb_treinamento_ws.gold.tb_workshop
(
DATA_INSCRICAO  TIMESTAMP COMMENT 'Data da Criação do Lead'
,STATUS  STRING COMMENT 'Status do Lead'
,FLAG_CONHECIMENTO_AZURE STRING COMMENT 'Flag Conhecimento em Azure'
,FLAG_TRABALHANDO STRING COMMENT 'Flag Trabalha, SIM ou NÃO'
,FLAG_COMO_CONHECEU_WORKSHOP STRING COMMENT 'Flag Como Conheceu o Workshop'
,FLAG_FORMA_PAGAMENTO STRING COMMENT 'Flag Forma de Pagamento'
,ID_INSCRICAO STRING COMMENT 'Id gerado automático'
,DATA_CRICAO_REGISTRO TIMESTAMP COMMENT 'Data de criação do registro'
)
using delta
--location '/mnt/dados/tb_workshop/'
comment 'Tabela de dados do produto WORKSHOP na camada GOLD'

