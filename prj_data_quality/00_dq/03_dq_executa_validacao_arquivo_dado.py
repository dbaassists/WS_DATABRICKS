# Databricks notebook source
import sys
sys.path.insert(0,"../00_dq/")
from dq_validacao_arquivo_dado import valida_dados_arquivo

# COMMAND ----------

dbutils.widgets.text("idprojeto", "1")
dbutils.widgets.text("idfontedado", "1")

# COMMAND ----------

idprojeto = dbutils.widgets.get("idprojeto")
idfontedado = dbutils.widgets.get("idfontedado")
nomEtapa = 'ETAPA DE VALIDAÇÃO DOS DADOS DO ARQUIVO.'
indice = 1

erro = valida_dados_arquivo(idprojeto, idfontedado, nomEtapa, indice)

print("Erro: "+ str(erro))

# COMMAND ----------


