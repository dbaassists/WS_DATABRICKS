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

SELECT 
ETAPA,
CATEGORIA,
SUM(TOTAL) TOTAL
FROM
(
  SELECT     
    1 ETAPA,
    CASE WHEN STATUS IN (
                        'PAGO'
                        ,'CONTATO REALIZADO'
                        ,'PROBLEMA CADASTRO'
                        ,'ORGANIZADOR'
                        ) THEN '1 - MANIFESTOU INTERESSE NO WORKSHOP (Conscientização)'
    END CATEGORIA,
    COUNT(1) TOTAL 
    
  FROM adb_treinamento_ws.gold.tb_workshop

  WHERE STATUS  IN (
                        'PAGO'
                        ,'CONTATO REALIZADO'
                        ,'PROBLEMA CADASTRO'
                        ,'ORGANIZADOR'
                        ) 

  GROUP BY STATUS

UNION ALL

SELECT 
  2 ETAPA,
  CASE 
  WHEN STATUS IN (
                    'PAGO'
                    ,'CONTATO REALIZADO'
                    ,'ORGANIZADOR'
                  ) THEN '2 - POTENCIAIS CLIENTES (Interesse)'
  END CATEGORIA,
  
  SUM(CASE 
    
  WHEN STATUS IN (
                    'PAGO'
                    ,'CONTATO REALIZADO'
                    ,'ORGANIZADOR'
                  ) THEN 1
  END) TOTAL 
  
FROM adb_treinamento_ws.gold.tb_workshop

WHERE STATUS IN (
                    'PAGO'
                    ,'CONTATO REALIZADO'
                    ,'ORGANIZADOR'
                  ) 

GROUP BY STATUS          

UNION ALL

SELECT 
  3 ETAPA,
  CASE 
  WHEN STATUS IN (
                  'PAGO'
                  ,'CONTATO REALIZADO'
                  ) THEN '3 - CONTATO REALIZADO (Consideração)'
  END CATEGORIA,
  
  SUM(CASE 
    
  WHEN STATUS IN (
                  'PAGO'
                  ,'CONTATO REALIZADO'
                  ) THEN 1
  END) TOTAL 
  
FROM adb_treinamento_ws.gold.tb_workshop

WHERE STATUS  IN (
                  'PAGO'
                  ,'CONTATO REALIZADO'
                  )

GROUP BY STATUS          

UNION ALL

SELECT 
  4 ETAPA,
  CASE 
  WHEN STATUS IN (
                  'PAGO'
                  ,'ORGANIZADOR'
                  ) THEN '4 - MATRICULA REALIZADA (Decisão/Cliente)'
  END CATEGORIA,
  
  SUM(CASE 
  
  WHEN STATUS IN (
                  'PAGO'
                  ,'ORGANIZADOR'
              ) THEN 1

  END) TOTAL 
  
FROM adb_treinamento_ws.gold.tb_workshop

WHERE STATUS IN (
                  'PAGO'
                  ,'ORGANIZADOR'
                  )

GROUP BY STATUS

) A

GROUP BY ETAPA,CATEGORIA

ORDER BY CATEGORIA

-- COMMAND ----------

SELECT FLAG_CONHECIMENTO_AZURE, COUNT(1) TOTAL 
FROM adb_treinamento_ws.gold.tb_workshop
GROUP BY FLAG_CONHECIMENTO_AZURE

-- COMMAND ----------

SELECT FLAG_TRABALHANDO, COUNT(1) TOTAL 
FROM adb_treinamento_ws.gold.tb_workshop
GROUP BY FLAG_TRABALHANDO

-- COMMAND ----------

SELECT FLAG_COMO_CONHECEU_WORKSHOP, COUNT(1) TOTAL 
FROM adb_treinamento_ws.gold.tb_workshop
GROUP BY FLAG_COMO_CONHECEU_WORKSHOP
ORDER BY TOTAL

-- COMMAND ----------

SELECT FLAG_FORMA_PAGAMENTO, COUNT(1) TOTAL 
FROM adb_treinamento_ws.gold.tb_workshop
GROUP BY FLAG_FORMA_PAGAMENTO
