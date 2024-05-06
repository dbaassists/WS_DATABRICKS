import sys
import pandas as pd
sys.path.insert(0,"/Workspace/util/")
from envia_email import envia_email
from registra_log import registra_log
from conexao_db import conexao_db

def valida_dados_arquivo(idprojeto, idfontedado, nomEtapa, indice):

    try:

        ##########################################################################################################
        # VARIÁVEIS USADAS NA VALIDAÇÃO DO ARQUIVO
        ##########################################################################################################

        contador = 0
        msg = f'Prezado usuário, \n\nSegue o resumo do processo de validação do arquivo de cadastro de Data Quality.\n\n'        

        cnxn = conexao_db()
        cur = cnxn.cursor()

        queryContratoDados = f"""SELECT 
                                FD.NomTabela
                                , FD.NomFonteDados
                                , P.NomProjeto
                                , FD.NomAbaExcel
                                , FD.DataLakeFolder
                                , FD.DscExtensaoArquivo
                                , FD.TpoDelimitadorArquivo
                                , FD.NomEncodingArquivo
                                , EFD.NumSeqColunaTabela
                                , EFD.NomColuna
                                , EFD.TpoDado
                                , EFD.FlagColunaNula
                                , EFD.FlagColunaObrigatoria
                                , EFD.FlagColunaChave
                                , EFD.TpoDadoArmazenado
                                FROM dq.TB_ESTRUTURA_FONTE_DADO EFD
                                INNER JOIN dq.TB_FONTE_DADO FD
                                ON EFD.IdFonteDados = FD.IdFonteDados
                                INNER JOIN dq.TB_PROJETO P
                                ON FD.IdProjeto = P.IdProjeto
                                WHERE EFD.IdProjeto = '{idprojeto}'
                                AND FD.IdFonteDados = '{idfontedado}';"""

        dfContratoDados = pd.read_sql(queryContratoDados, cnxn)

        v_diretorio = dfContratoDados["DataLakeFolder"].drop_duplicates(keep="last").head(1).values[0]
        v_arquivo = dfContratoDados["NomFonteDados"].drop_duplicates(keep="last").head(1).values[0]
        v_extensao = dfContratoDados["DscExtensaoArquivo"].drop_duplicates(keep="last").head(1).values[0]
        v_nomeArquivo = v_diretorio + '\\' + v_arquivo + '.' + v_extensao 
        v_aba = dfContratoDados["NomAbaExcel"].drop_duplicates(keep="last").head(1).values[0]
        v_delimitador = dfContratoDados["TpoDelimitadorArquivo"].drop_duplicates(keep="last").head(1).values[0]
        v_encoding = dfContratoDados["NomEncodingArquivo"].drop_duplicates(keep="last").head(1).values[0]
        v_NomeProjeto = dfContratoDados["NomProjeto"].drop_duplicates(keep="last").head(1).values[0]
        v_FonteDados = dfContratoDados["NomFonteDados"].drop_duplicates(keep="last").head(1).values[0]

        ##########################################################################################################
        # VALIDAÇÃO PELO TIPO DE EXTENSÃO DO ARQUIVO
        ##########################################################################################################      
        
        if (v_extensao == 'csv') | (v_extensao == 'txt'):

            print(v_extensao)

            dfArquivoImportado = pd.read_csv(
                v_nomeArquivo,
                sep=v_delimitador,
                encoding=v_encoding,
                header=0
            )  

        elif v_extensao == 'xslx':

            print(v_extensao)            

            dfArquivoImportado = pd.read_excel(
                v_nomeArquivo,
                sheet_name=v_aba,
                sep=v_delimitador,
                encoding=v_encoding,
                header=0
            )

        ##########################################################################################################
        # VERIFICANDO SE AS COLUNAS DO ARQUIVO ESTÃO DE ACORDO COM O CONTRATO DE DADOS
        ##########################################################################################################

        dfColunaContrato = pd.DataFrame(dfContratoDados['NomColuna'])
        dfColunaArquivoImportado = pd.DataFrame(pd.Series(dfArquivoImportado.columns, name='NomColuna'))
        serie_df1 = dfColunaContrato['NomColuna']
        serie_df2 = dfColunaArquivoImportado['NomColuna']

        for i in range(len(serie_df1)):

            valor_df1 = serie_df1.iloc[i]

            valor_df2 = serie_df2.iloc[i]

            if valor_df1 != valor_df2:

                contador += 1

                msg += f"* {contador} - Durante a validação do arquivo {v_arquivo + '.' + v_extensao}, foi identificado que o nome da coluna {valor_df2} é diferente do cadastro no Data Quality. O nome da coluna deverá ser {valor_df1}. Essa é a {i+1} coluna do arquivo {v_arquivo}. \n"  

        ##########################################################################################################
        # VERIFICANDO SE EXISTE ALGUMA COLUNA NO ARQUIVO QUES ESTÁ SENDO IMPORTADO NÃO ESTÁ NO CONTRATO DE DADOS
        ##########################################################################################################

        #listaColunasContrato = dfColunaContrato['NomColuna'].to_list()

        for index,row in dfColunaArquivoImportado.iterrows():

            coluna = row['NomColuna']

            if coluna not in dfColunaContrato['NomColuna'].to_list():

                contador += 1

                msg += f"* {contador} - Durante a validação do arquivo {v_arquivo + '.' + v_extensao}, foi identificado que a coluna {coluna} não está cadastrada Data Quality para a fonte de dados {v_arquivo + '.' + v_extensao}. O nome da coluna não encontrada é {coluna} e ela está na {index+1} posição do arquivo {v_arquivo}. \n"  

        ##########################################################################################################
        # VERIFICANDO OS REGISTROS BASEADO NO DATATYPE CADASTRADO
        ##########################################################################################################

        for col in dfArquivoImportado.columns:
        
            #colunasArquivoImportado = dfArquivoImportado[col].fillna(-99).to_list()
            tipoDado = dfContratoDados[dfContratoDados['NomColuna'] == col]['TpoDado'].to_string(index=False)

            for seq, elemento in enumerate(dfArquivoImportado[col].fillna(-99).to_list()):

                if tipoDado == 'INT':

                    try:

                        pd.to_numeric(elemento, downcast='integer', errors='raise')

                    except ValueError as e:

                        contador += 1

                        msg += f"* {contador} - Durante a importação do arquivo {v_arquivo + '.' + v_extensao}, na linha {str(seq+1)} a coluna {col} possui um valor inválido. Valor encontrado: {str(elemento)}. \n" 

                elif tipoDado == 'FLOAT':

                    try:

                        pd.to_numeric(elemento, downcast='float', errors='raise')

                    except ValueError as e:

                        contador += 1

                        msg += f"* {contador} - Durante a importação do arquivo {v_arquivo + '.' + v_extensao}, na linha {str(seq+1)} a coluna {col} possui um valor inválido. Valor encontrado: {str(elemento)}. \n" 

        ##########################################################################################################
        # VERIFICANDO COLUNAS QUE NÃO ACEITAM VALORES NULOS
        ##########################################################################################################

        for col in dfContratoDados[dfContratoDados['FlagColunaNula'] == 1]['NomColuna']:
        
            #colunasNula = dfArquivoImportado[col].to_list()

            for seqColunaNula, tpColunaNula in enumerate(dfArquivoImportado[col].to_list()):

                if pd.isna(tpColunaNula):
                    
                    contador += 1

                    msg += f"* {contador} - Durante a importação do arquivo {v_arquivo + '.' + v_extensao}, na linha {str(seqColunaNula+1)} a coluna {col} tem o seu preenchimento obrigatório e no arquivo {v_arquivo + '.' + v_extensao} ela está apresentando valores ausentes.\n" 

        ##########################################################################################################
        # VERIFICANDO COLUNAS QUE POSSUEM SEU PREENCHIMENTO OBRIGATÓRIO
        ##########################################################################################################

        for col in dfContratoDados[dfContratoDados['FlagColunaObrigatoria'] == 1]['NomColuna']:

            #colunasObrigatoria = dfArquivoImportado[col].to_list()

            for seqColunaObrigatoria, tpColunaObrigatoria in enumerate(dfArquivoImportado[col].to_list()):

                if pd.isna(tpColunaObrigatoria):
                    
                    contador += 1

                    msg += f"* {contador} - Durante a importação do arquivo {v_arquivo + '.' + v_extensao}, na linha {str(seqColunaObrigatoria+1)} a coluna {col} tem o seu preenchimento obrigatório e no arquivo {v_arquivo + '.' + v_extensao} ela está apresentando valores ausentes.\n" 

        ##########################################################################################################
        # VERIFICANDO COLUNAS CHAVE DA TABELA
        ##########################################################################################################

        for col in dfContratoDados[dfContratoDados['FlagColunaChave'] == 1]['NomColuna']:
        
            repetido = dfArquivoImportado.groupby(col, as_index=False).agg(Ocorrencia= (col, 'count'))

            for coluna in repetido[repetido['Ocorrencia'] > 1][col]:

                for seqColunaPK, tpColunaPK in enumerate(dfArquivoImportado[dfArquivoImportado[col] == coluna][col].to_list()):

                    contador += 1

                    msg += f"* {contador} - Durante a importação do arquivo {v_arquivo + '.' + v_extensao}, na linha {str(seqColunaPK+1)} a coluna {col} não permite valores repetidos. Foram encontrados os valores {str(tpColunaPK)} repetidos na coluna {col}.\n" 


        ##########################################################################################################
        # VALIDANDO COLUNA TIPO E-MAIL
        ##########################################################################################################

        for col in dfContratoDados[dfContratoDados['TpoDadoArmazenado'] == 'EMAIL']['NomColuna']:

            #colunasEmail = dfArquivoImportado[col].fillna(-99).to_list()

            for seq, elemento in enumerate(dfArquivoImportado[col].fillna(-99).to_list()):

                if ('@' not in elemento):

                    contador += 1

                    msg += f"* {contador} - Durante a importação do arquivo {v_arquivo + '.' + v_extensao}, na linha {str(seq+1)} foi identificado que existe um registro fora do padrão permitido. Os registros dessa coluna devem ser e-mail. Segue o valor encontrado {elemento}. \n"


        ##########################################################################################################
        # VALIDANDO COLUNA TIPO DATA
        ##########################################################################################################

        for col in dfContratoDados[dfContratoDados['TpoDadoArmazenado'] == 'DATA']['NomColuna']:
            
            for seq, elemento in enumerate(dfArquivoImportado[col].fillna(-99).to_list()):                

                try:

                    pd.to_datetime(elemento, format='%d/%m/%Y')

                except Exception as e:

                    contador += 1

                    msg += f"* {contador} - Durante a importação do arquivo {v_arquivo + '.' + v_extensao}, na linha {str(seq+1)} foi identificado que existe um registro fora do padrão permitido. Os registros dessa coluna devem ser data. Segue o valor encontrado {elemento}. \n"

        ##########################################################################################################
        # VALIDANDO COLUNA TIPO DATETIME
        ##########################################################################################################

        for col in dfContratoDados[dfContratoDados['TpoDadoArmazenado'] == 'DATETIME']['NomColuna']:
            
            for seq, elemento in enumerate(dfArquivoImportado[col].fillna(-99).to_list()):                

                try:

                    pd.to_datetime(elemento, format='%Y-%m-%d %H:%M:%S')                    

                except Exception as e:

                    contador += 1

                    msg += f"* {contador} - Durante a importação do arquivo {v_arquivo + '.' + v_extensao}, na linha {str(seq+1)} foi identificado que existe um registro fora do padrão permitido. Os registros dessa coluna devem ser no formato data como YYYY-MM-DD HH:MM:SS. Segue o valor encontrado {elemento}. \n"


        ##########################################################################################################
        # FIM VALIDAÇÃO ARQUIVO
        ##########################################################################################################

        erro = contador

        if contador == 0:

            msg += f'* {contador} - Não foi encontrada nenhuma inconsistência.'

            fromaddr = "dbaassists@gmail.com"
            toaddr = "dbaassists@gmail.com"
            assunto = "Data Quality - Inconsistências"

            envia_email(fromaddr, toaddr, assunto, msg)              

        elif contador == 1:

            msg += f'\nFoi identificada {contador} inconsistência no seu arquivo.'    

            fromaddr = "dbaassists@gmail.com"
            toaddr = "dbaassists@gmail.com"
            assunto = "Data Quality - Inconsistências"

            envia_email(fromaddr, toaddr, assunto, msg)                      

        else:

            msg += f'\nForam identificadas {contador} inconsistências no seu arquivo.'   

            fromaddr = "dbaassists@gmail.com"
            toaddr = "dbaassists@gmail.com"
            assunto = "Data Quality - Inconsistências"

            envia_email(fromaddr, toaddr, assunto, msg)                       

        print(msg)

    except Exception as e:

        registra_log(
            v_arquivo,
            v_NomeProjeto,
            v_FonteDados,
            nomEtapa,
            "E",
            f"{indice} - OCORREU UM ERRO: " + str(e)
        )

    return erro

    
