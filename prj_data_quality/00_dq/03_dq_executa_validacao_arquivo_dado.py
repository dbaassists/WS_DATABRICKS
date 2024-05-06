# Databricks notebook source
import sys
sys.path.insert(0,"/Workspace/util/")
from conexao_db import conexao_db
from registra_log import registra_log
from dq_validacao_arquivo_dado import valida_dados_arquivo
import pyodbc as pc

# COMMAND ----------

dbutils.widgets.text("idprojeto", "")
dbutils.widgets.text("idfontedado", "")

# COMMAND ----------

idprojeto = dbutils.widgets.get("idprojeto")
idfontedado = dbutils.widgets.get("idfontedado")
nomEtapa = 'ETAPA DE VALIDAÇÃO DOS DADOS DO ARQUIVO.'
indice = 1

valida_dados_arquivo(idprojeto, idfontedado, nomEtapa, indice)
