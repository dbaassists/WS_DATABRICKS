# Databricks notebook source
import pandas as pd

dfArquivoImportado = pd.read_csv(
                '/dbfs/mnt/dados/fontes/cliente/01_clientes.csv',
                sep=';',
                encoding='latin-1',
                header=0
            )  

dfArquivoImportado['data2'] = pd.to_datetime(dfArquivoImportado['DATA_NASCIMENTO'], format='%Y-%m-%d')

dfArquivoImportado

#dfArquivoImportado.astype(str)

#dfArquivoImportado.info()

# COMMAND ----------


