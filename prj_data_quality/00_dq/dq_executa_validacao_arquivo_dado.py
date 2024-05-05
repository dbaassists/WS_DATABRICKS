import sys
sys.path.insert(0,"/Workspace/util/")
from conexao_db import conexao_db
sys.path.insert(0,"/00_dq/")
from _0_dq_cadastro_fonte import importa_planilha_dq
import pyodbc as pc

idprojeto = 1
idfontedado = 1
nomEtapa = 'ETAPA DE VALIDAÇÃO DOS DADOS DO ARQUIVO.'
indice = 1

valida_dados_arquivo(idprojeto, idfontedado, nomEtapa, indice)