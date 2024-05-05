import sys
import os
import pandas as pd
import pyodbc as pc

from envia_email import envia_email
from registra_log import registra_log
from dq_cadastro_fonte import importa_planilha_dq
from conexao_db import conexao_db

sys.path.insert(0, "/Workspace/util/")

diretorio = '/dbfs/mnt/dados/data_quality/'
abaProjeto = 'Projeto'
abaFonteDado = 'FonteDado'
abaEstruturaFonteDado = 'EstruturaFonteDado'

for arquivo in os.listdir(diretorio):

    path = os.path.join(diretorio, arquivo)

    if  os.path.isfile(path):

        importa_planilha_dq(path,abaProjeto,abaFonteDado,abaEstruturaFonteDado)    