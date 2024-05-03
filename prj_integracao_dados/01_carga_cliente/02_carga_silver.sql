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

MERGE INTO adb_treinamento_ws.silver.TB_CLIENTE AS DESTINO
USING (
  
SELECT 
CODIGO_CLIENTE CodCliente
,UPPER(NOME_CLIENTE) NomCliente
,CAST(DATA_NASCIMENTO AS DATE) DtaNascimentoCliente
,IDADE_CLIENTE NumIdadeCliente
,UPPER(DESCRICAO_ENDERECO) DscEnderecoCliente
,NUMERO_ENDERECO NumEnderecoCliente
,UPPER(NOME_CIDADE) NomCidadeCliente
,NUMERO_TELEFONE NumTelefoneCliente
,EMAIL_CLIENTE DscEmailCliente
,UPPER(OCUPACAO_CLIENTE) DscOcupacaoCliente
,row_number() OVER(PARTITION BY CODIGO_CLIENTE ORDER BY DATA_HORA_CARGA DESC) ID
FROM adb_treinamento_ws.bronze.TB_CLIENTE
QUALIFY ID = 1

) AS ORIGEM
ON DESTINO.CodCliente = ORIGEM.CodCliente
WHEN MATCHED THEN
UPDATE SET 

DESTINO.NomCliente = ORIGEM.NomCliente
,DESTINO.DtaNascimentoCliente = ORIGEM.DtaNascimentoCliente
,DESTINO.NumIdadeCliente = ORIGEM.NumIdadeCliente
,DESTINO.DscEnderecoCliente = ORIGEM.DscEnderecoCliente
,DESTINO.NumEnderecoCliente = ORIGEM.NumEnderecoCliente
,DESTINO.NomCidadeCliente = ORIGEM.NomCidadeCliente
,DESTINO.NumTelefoneCliente = ORIGEM.NumTelefoneCliente
,DESTINO.DscEmailCliente = ORIGEM.DscEmailCliente
,DESTINO.DscOcupacaoCliente = ORIGEM.DscOcupacaoCliente
,DESTINO.DthAlteracao = from_utc_timestamp(current_timestamp(), 'GMT-3')

WHEN NOT MATCHED THEN
INSERT (
CodCliente
,NomCliente
,DtaNascimentoCliente
,NumIdadeCliente
,DscEnderecoCliente
,NumEnderecoCliente
,NomCidadeCliente
,NumTelefoneCliente
,DscEmailCliente
,DscOcupacaoCliente
,DthInclusao
,DthAlteracao
)
VALUES (
ORIGEM.CodCliente
,ORIGEM.NomCliente
,ORIGEM.DtaNascimentoCliente
,ORIGEM.NumIdadeCliente
,ORIGEM.DscEnderecoCliente
,ORIGEM.NumEnderecoCliente
,ORIGEM.NomCidadeCliente
,ORIGEM.NumTelefoneCliente
,ORIGEM.DscEmailCliente
,ORIGEM.DscOcupacaoCliente
,from_utc_timestamp(current_timestamp(), 'GMT-3')
,from_utc_timestamp(current_timestamp(), 'GMT-3')
);
