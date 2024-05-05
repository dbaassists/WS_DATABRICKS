# Databricks notebook source
# MAGIC %run "../00_dq/00_dq_cadastro_fonte"
# MAGIC

# COMMAND ----------

import sys
sys.path.insert(0,"/Workspace/util/")
from conexao_db import conexao_db
sys.path.insert(0,"/00_dq/")
from dq_cadastro_fonte import importa_planilha_dq
import pyodbc as pc

# COMMAND ----------

diretorio = '/dbfs/mnt/dados/data_quality/'
abaProjeto = 'Projeto'
abaFonteDado = 'FonteDado'
abaEstruturaFonteDado = 'EstruturaFonteDado'

for arquivo in os.listdir(diretorio):

    path = os.path.join(diretorio, arquivo)

    if  os.path.isfile(path):

        importa_planilha_dq(path,abaProjeto,abaFonteDado,abaEstruturaFonteDado)    

# COMMAND ----------


