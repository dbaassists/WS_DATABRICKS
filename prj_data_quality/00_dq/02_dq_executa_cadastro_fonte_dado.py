# Databricks notebook source
# MAGIC %run "/exemplos/data_quality/00_dq_cadastro_fonte"

# COMMAND ----------

import sys
sys.path.insert(0,"../util/")
from envia_email import envia_email
from registra_log import registra_log

# COMMAND ----------

diretorio = '/dbfs/mnt/dados/data_quality/'
abaProjeto = 'Projeto'
abaFonteDado = 'FonteDado'
abaEstruturaFonteDado = 'EstruturaFonteDado'

for arquivo in os.listdir(diretorio):

    path = os.path.join(diretorio, arquivo)

    if  os.path.isfile(path):

        importa_planilha_dq(path,abaProjeto,abaFonteDado,abaEstruturaFonteDado)    
