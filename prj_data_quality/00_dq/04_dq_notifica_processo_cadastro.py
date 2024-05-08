# Databricks notebook source
import sys
sys.path.insert(0,"/Workspace/util/")
from envia_email import envia_email

# COMMAND ----------

dbutils.widgets.text("fromaddr", "1")
dbutils.widgets.text("toaddr", "1")
dbutils.widgets.text("assunto", "")
dbutils.widgets.text("msg", "")

# COMMAND ----------

fromaddr = dbutils.widgets.get("idprojeto")
toaddr = dbutils.widgets.get("toaddr")
assunto = dbutils.widgets.get("assunto")
msg = dbutils.widgets.get("msg") 

envia_email(fromaddr, toaddr, assunto, msg)
