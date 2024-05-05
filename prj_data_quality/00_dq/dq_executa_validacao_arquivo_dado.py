import sys
sys.path.insert(0,"../util/")
from envia_email import envia_email
from registra_log import registra_log
from dq_validacao_arquivo_dado import dq_validacao_arquivo_dado

idprojeto = 1
idfontedado = 1
nomEtapa = 'ETAPA DE VALIDAÇÃO DOS DADOS DO ARQUIVO.'
indice = 1

valida_dados_arquivo(idprojeto, idfontedado, nomEtapa, indice)