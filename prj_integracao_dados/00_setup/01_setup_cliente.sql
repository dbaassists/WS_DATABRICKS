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

-- DBTITLE 1,01 - Criação da Tabela "TB_CLIENTE" na camada Bronze
drop table if exists adb_treinamento_ws.bronze.TB_CLIENTE;
create table if not exists adb_treinamento_ws.bronze.TB_CLIENTE
(
CODIGO_CLIENTE INT NOT NULL  COMMENT 'Codigo do Cliente',
NOME_CLIENTE STRING NOT NULL COMMENT 'Nome do Cliente',
DATA_NASCIMENTO STRING NOT NULL  COMMENT 'Data Nascimento do Cliente',
IDADE_CLIENTE STRING NOT NULL  COMMENT 'Idade do Cliente',
DESCRICAO_ENDERECO STRING NOT NULL  COMMENT 'Endereço do Cliente',
NUMERO_ENDERECO STRING NOT NULL  COMMENT 'Número do Endereço do Cliente',
NOME_CIDADE STRING NOT NULL  COMMENT 'Cidade do Cliente',
NUMERO_TELEFONE STRING NOT NULL COMMENT 'Número do Telefone do Cliente',
EMAIL_CLIENTE STRING NOT NULL COMMENT 'E-mail do Cliente',
OCUPACAO_CLIENTE STRING NOT NULL COMMENT 'Ocupação do Cliente',
DATA_HORA_CARGA STRING NOT NULL COMMENT 'Data e Hora da Carga'
)
using delta
--location '/mnt/dados/bronze/tb_workshop/'
comment 'Tabela de dados de CLIENTE na camada BRONZE'

-- COMMAND ----------

-- DBTITLE 1,02 - Criação da Tabela "TB_CLIENTE" na camada Silver
drop table if exists adb_treinamento_ws.silver.TB_CLIENTE;
create table if not exists adb_treinamento_ws.silver.TB_CLIENTE
(
CodCliente  int COMMENT 'Codigo do Cliente'
,NomCliente STRING COMMENT 'Nome do Cliente'
,DtaNascimentoCliente date COMMENT 'Data Nascimento do Cliente'
,NumIdadeCliente int COMMENT 'Idade do Cliente'
,DscEnderecoCliente STRING COMMENT 'Endereço do Cliente'
,NumEnderecoCliente STRING COMMENT 'Número do Endereço do Cliente'
,NomCidadeCliente STRING COMMENT 'Cidade do Cliente'
,NumTelefoneCliente STRING COMMENT 'Número do Telefone do Cliente'
,DscEmailCliente STRING COMMENT 'E-mail do Cliente'
,DscOcupacaoCliente STRING COMMENT 'Ocupação do Cliente'
,DthInclusao TIMESTAMP COMMENT 'Data de criação do registro'
,DthAlteracao TIMESTAMP COMMENT 'Data de alteração do registro'
)
using delta
--location '/mnt/dados/tb_workshop/'
comment 'Tabela de dados de CLIENTE na camada SILVER'

-- COMMAND ----------

-- DBTITLE 1,03 - Criação da Tabela "DIM_CLIENTE" na camada Gold
drop table if exists adb_treinamento_ws.gold.DIM_CLIENTE;
create table if not exists adb_treinamento_ws.gold.DIM_CLIENTE
(
SKCliente STRING  COMMENT 'SK da Dimensão Cliente'
,CodCliente  int COMMENT 'Codigo do Cliente'
,NomCliente STRING COMMENT 'Nome do Cliente'
,DtaNascimentoCliente date COMMENT 'Data Nascimento do Cliente'
,NumIdadeCliente int COMMENT 'Idade do Cliente'
,DscEnderecoCliente STRING COMMENT 'Endereço do Cliente'
,NumEnderecoCliente STRING COMMENT 'Número do Endereço do Cliente'
,NomCidadeCliente STRING COMMENT 'Cidade do Cliente'
,NumTelefoneCliente STRING COMMENT 'Número do Telefone do Cliente'
,DscEmailCliente STRING COMMENT 'E-mail do Cliente'
,DscOcupacaoCliente STRING COMMENT 'Ocupação do Cliente'
,DthInicioVigencia TIMESTAMP COMMENT 'Data de inicio da vigência do registro'
,DthFimVigencia TIMESTAMP COMMENT 'Data de fim da vigência do registro'
,DthInclusao TIMESTAMP COMMENT 'Data de criação do registro'
,DthAlteracao TIMESTAMP COMMENT 'Data de alteração do registro'
)
using delta
--location '/mnt/dados/tb_workshop/'
comment 'Tabela de dados de CLIENTE (Dimensão) na camada GOLD'
