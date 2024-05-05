import sys
import os
sys.path.insert(0,"../util/")
from envia_email import envia_email
from registra_log import registra_log
from dq_cadastro_fonte import dq_cadastro_fonte

diretorio = '/dbfs/mnt/dados/data_quality/'
abaProjeto = 'Projeto'
abaFonteDado = 'FonteDado'
abaEstruturaFonteDado = 'EstruturaFonteDado'

for arquivo in os.listdir(diretorio):

    path = os.path.join(diretorio, arquivo)

    if  os.path.isfile(path):

        importa_planilha_dq(path,abaProjeto,abaFonteDado,abaEstruturaFonteDado)    