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

DROP TABLE IF EXISTS temp_dim_cliente;
CREATE TABLE temp_dim_cliente
USING DELTA
AS
SELECT 
SHA2(CONCAT( ORIGEM.CodCliente,ORIGEM.NomCliente ), 256) SKCliente
,ORIGEM.CodCliente
,ORIGEM.NomCliente
,ORIGEM.DtaNascimentoCliente
,ORIGEM.NumIdadeCliente
,ORIGEM.DscEnderecoCliente
,ORIGEM.NumEnderecoCliente
,ORIGEM.NomCidadeCliente
,ORIGEM.NumTelefoneCliente
,ORIGEM.DscEmailCliente
,ORIGEM.DscOcupacaoCliente
,ORIGEM.DthInclusao
,ORIGEM.DthAlteracao
,from_utc_timestamp(current_timestamp(), 'GMT-3') DthInicioVigencia
,from_utc_timestamp(current_timestamp(), 'GMT-3') DthFimVigencia
,CASE
WHEN ORIGEM.CodCliente = DESTINO.CodCliente AND ORIGEM.NomCliente = DESTINO.NomCliente
THEN 'A'
WHEN ORIGEM.CodCliente = DESTINO.CodCliente AND ORIGEM.NomCliente != DESTINO.NomCliente
THEN 'V'
ELSE 'I'
END TP_ACAO

FROM adb_treinamento_ws.silver.TB_CLIENTE ORIGEM

LEFT JOIN (SELECT CodCliente, NomCliente FROM adb_treinamento_ws.gold.DIM_CLIENTE
          WHERE DthFimVigencia IS NULL) DESTINO

ON ORIGEM.CodCliente = DESTINO.CodCliente

-- COMMAND ----------

INSERT INTO adb_treinamento_ws.gold.DIM_CLIENTE
(SKCliente
,CodCliente
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
,DthInicioVigencia
,DthFimVigencia 
)

SELECT 

ORIGEM.SKCliente
,ORIGEM.CodCliente
,ORIGEM.NomCliente
,ORIGEM.DtaNascimentoCliente
,ORIGEM.NumIdadeCliente
,ORIGEM.DscEnderecoCliente
,ORIGEM.NumEnderecoCliente
,ORIGEM.NomCidadeCliente
,ORIGEM.NumTelefoneCliente
,ORIGEM.DscEmailCliente
,ORIGEM.DscOcupacaoCliente
,ORIGEM.DthInclusao
,ORIGEM.DthAlteracao
,ORIGEM.DthInicioVigencia
,null DthFimVigencia

FROM temp_dim_cliente ORIGEM

WHERE TP_ACAO  = 'I'

-- COMMAND ----------


MERGE INTO adb_treinamento_ws.gold.dim_cliente AS DESTINO
USING (
  
SELECT 

ORIGEM.SKCliente
,ORIGEM.CodCliente
,ORIGEM.NomCliente
,ORIGEM.DtaNascimentoCliente
,ORIGEM.NumIdadeCliente
,ORIGEM.DscEnderecoCliente
,ORIGEM.NumEnderecoCliente
,ORIGEM.NomCidadeCliente
,ORIGEM.NumTelefoneCliente
,ORIGEM.DscEmailCliente
,ORIGEM.DscOcupacaoCliente
,ORIGEM.DthInclusao
,ORIGEM.DthAlteracao
,ORIGEM.DthInicioVigencia
,null DthFimVigencia

FROM temp_dim_cliente ORIGEM

WHERE TP_ACAO  = 'A'

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


-- COMMAND ----------


MERGE INTO adb_treinamento_ws.gold.dim_cliente AS DESTINO
USING (
  
SELECT 

ORIGEM.SKCliente
,ORIGEM.CodCliente
,ORIGEM.DthFimVigencia

FROM temp_dim_cliente ORIGEM

WHERE TP_ACAO  = 'V'

) AS ORIGEM

ON DESTINO.CodCliente = ORIGEM.CodCliente

WHEN MATCHED THEN
UPDATE SET 

DESTINO.CodCliente = ORIGEM.CodCliente
,DESTINO.DthFimVigencia = ORIGEM.DthFimVigencia


-- COMMAND ----------

INSERT INTO adb_treinamento_ws.gold.DIM_CLIENTE
(SKCliente
,CodCliente
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
,DthInicioVigencia
,DthFimVigencia 
)

SELECT 

ORIGEM.SKCliente
,ORIGEM.CodCliente
,ORIGEM.NomCliente
,ORIGEM.DtaNascimentoCliente
,ORIGEM.NumIdadeCliente
,ORIGEM.DscEnderecoCliente
,ORIGEM.NumEnderecoCliente
,ORIGEM.NomCidadeCliente
,ORIGEM.NumTelefoneCliente
,ORIGEM.DscEmailCliente
,ORIGEM.DscOcupacaoCliente
,ORIGEM.DthInclusao
,ORIGEM.DthAlteracao
,ORIGEM.DthInicioVigencia
,null DthFimVigencia

FROM temp_dim_cliente ORIGEM

WHERE TP_ACAO  = 'V'

-- COMMAND ----------

-- DBTITLE 1,3 - Aplicando Optimize em uma Tabela
optimize adb_treinamento_ws.gold.tb_workshop;
