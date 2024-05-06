# Databricks notebook source
import sys
sys.path.insert(0,"/Workspace/Repos/ws_projeto_azure/WS_DATABRICKS_20240505/prj_data_quality/00_dq/dq_validacao_arquivo_dado")
from dq_validacao_arquivo_dado import valida_dados_arquivo

# COMMAND ----------

dbutils.widgets.text("idprojeto", "1")
dbutils.widgets.text("idfontedado", "1")

# COMMAND ----------

idprojeto = dbutils.widgets.get("idprojeto")
idfontedado = dbutils.widgets.get("idfontedado")
nomEtapa = 'ETAPA DE VALIDAÇÃO DOS DADOS DO ARQUIVO.'
indice = 1

contador = valida_dados_arquivo(idprojeto, idfontedado, nomEtapa, indice)

print(contador)
