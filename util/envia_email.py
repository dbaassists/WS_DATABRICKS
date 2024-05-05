# Databricks notebook source
import sys
sys.path.insert(0,"../util/")
from envia_email import envia_email

fromaddr = "dbaassists@gmail.com"
toaddr = "dbaassists@gmail.com"
assunto = "Erro Processamento Databricks - Inconsistências"
msg = "Prezado usuário,\n\nOcorreu um erro ao realizar o processamento do Job no Databricks. \n\nAtenciosamente,\n\nEquipe Suporte"

envia_email(fromaddr, toaddr, assunto, msg)

# COMMAND ----------

# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# MAGIC sudo apt-get update
# MAGIC sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17

# COMMAND ----------

import sys
sys.path.insert(0,"../util/")
from conexao_db import conexao_db
import pyodbc as pc

conexao_db()
