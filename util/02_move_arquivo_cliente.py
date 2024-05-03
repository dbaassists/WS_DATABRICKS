# Databricks notebook source
# MAGIC %md
# MAGIC Regras, Descrições (macro) e detalhes
# MAGIC
# MAGIC |N° Controle|Descrição|Dados
# MAGIC |---|---|---|
# MAGIC |01|Camada| Bronze - Silver - Gold |
# MAGIC |02|Data Criação| Quando o Fluxo foi Criado |
# MAGIC |03|Desenvolvida por| Quem desenvolver? |
# MAGIC |04|Objetivo| Por que foi criado? |
# MAGIC |05|Solicitante| Quem é o demandante? |
# MAGIC |06|Projeto| Atende algum projeto? |
# MAGIC |07|Nome do Pipeline| Esse notebool é usado em alguma Pipeline do Azure Data Factory? |
# MAGIC |08|Data Alteração| Foi alterado depois de criado? ||

# COMMAND ----------

# DBTITLE 1,1 - Definição de Bibliotecas
import zipfile  
import os
import shutil

# COMMAND ----------

# DBTITLE 1,2 - Definição de Variáveis
var_diretorio_arquivo  = '/dbfs/mnt/dados/landing_zone/cliente'
var_diretorio_arquivo_importado = var_diretorio_arquivo + '/importado'

# COMMAND ----------

# DBTITLE 1,3 - Script para Movimentar os Arquivos Importados
#Cria Lista de estruturas no Path data_lake
list_dir = os.listdir(var_diretorio_arquivo)
list_dir_dest = os.listdir(var_diretorio_arquivo_importado)

for est in list_dir:

  print(var_diretorio_arquivo + est)

  if os.path.isfile(var_diretorio_arquivo + '//' + est):

    if ((os.path.splitext(est)[1]=='.xlsx') | (os.path.splitext(est)[1]=='.csv')) and (os.path.isfile(f'/dbfs/mnt/dados/landing_zone/cliente/{est}')):
            
        # move para archiving
        shutil.move(f'{var_diretorio_arquivo}/{est}',f'{var_diretorio_arquivo_importado}/{est}')

  else:

    print('não eh arquivo')
