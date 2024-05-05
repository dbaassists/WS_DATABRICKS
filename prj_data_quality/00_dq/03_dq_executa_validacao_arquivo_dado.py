# Databricks notebook source
# MAGIC %run "/exemplos/data_quality/01_dq_validacao_arquivo_dado"

# COMMAND ----------

idprojeto = 1
idfontedado = 1
nomEtapa = 'ETAPA DE VALIDAÇÃO DOS DADOS DO ARQUIVO.'
indice = 1

valida_dados_arquivo(idprojeto, idfontedado, nomEtapa, indice)
