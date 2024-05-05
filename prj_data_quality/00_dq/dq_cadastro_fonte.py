import sys
sys.path.insert(0,"../util/")
from envia_email import envia_email
from registra_log import registra_log

#####################################################################################################################
# Número da Regra: 1
# Breve Descrição: Etapa destinada para criação de função Python responsável pelo registro dos logs de processamento
# Data Criação: 09/04/2024
# Criado por: Gabriel Quintella
#####################################################################################################################

def consulta_projeto(nomArquivo, nomeProjeto, nomEtapa,indice):

    try:

        cnxn = conexao_db()

        cur = cnxn.cursor()

        queryIdProjeto = f"""SELECT ISNULL(idProjeto,-1) idProjeto 
                            FROM [dq].[TB_PROJETO] 
                            WHERE [NomProjeto] = '{nomeProjeto}';"""

        dfNomeProjeto = pd.read_sql(queryIdProjeto, cnxn)

        cur.close()
        cnxn.close()

        if dfNomeProjeto.empty:

            idProjeto = -1

        else:

            idProjeto = dfNomeProjeto["idProjeto"].to_string(index=False)

        registra_log(
            nomArquivo,
            nomeProjeto,
            nomeProjeto,
            nomEtapa,
            "S",
            f"{indice} - CONSULTA DE PROJETO REALIZADO COM SUCESSO."
        )

    except pc.IntegrityError as e:

        registra_log(
            nomArquivo,
            nomeProjeto,
            nomeProjeto,
            nomEtapa,
            "E",
            f"{indice} - ERROR: INTEGRITY CONSTRAINT VIOLATION OCCURRED: " + str(e)
        )

    except pc.Error as e:

        registra_log(
            nomArquivo,
            nomeProjeto,
            nomeProjeto,
            nomEtapa,
            "E",
            f"{indice} - ERROR: FAILED TO INSERT DATA INTO THE DATABASE: " + str(e)
        )

    except Exception as e:

            registra_log(
                nomArquivo,
                nomeProjeto,
                nomeProjeto,
                nomEtapa,
                "E",
                f"{indice} - OCORREU UM ERRO AO CONSULTAR UM PROJETO. ERROR: " + str(e)
            )

    indice += 1

    return idProjeto, indice



#####################################################################################################################
# Número da Regra: 1
# Breve Descrição: 	Etapa destinada para criação de função Python responsável pela consulta da fonte de dado antes de cadastrar
# Data Criação: 09/04/2024
# Criado por: Gabriel Quintella
#####################################################################################################################

def consulta_fonte_dado(arquivo, idProjeto, nomeFonteDado, nomEtapa,indice):

    try:

        cnxn = conexao_db()
        cur = cnxn.cursor()

        queryIdFonteDados = f"""
        SELECT CAST(ISNULL(SUM(tFDado.idFonteDados),-1) AS INT) idFonteDados , tPro.NomProjeto
        FROM [dq].[TB_FONTE_DADO] tFDado
        inner join [dq].[TB_PROJETO] tPro
        on tFDado.IdProjeto = tPro.IdProjeto                            
        where tFDado.[NomFonteDados] = '{nomeFonteDado}'
        and tPro.[IdProjeto] = {idProjeto}
        GROUP BY tPro.NomProjeto,tPro.NomProjeto;
        """

        dfNomFonteDados = pd.read_sql(queryIdFonteDados, cnxn)

        idFonteDados = dfNomFonteDados["idFonteDados"].to_string(index=False)

        nomeProjeto = dfNomFonteDados["NomProjeto"].to_string(index=False)

        if dfNomFonteDados.empty:
            
            idFonteDados = -1

        else:

            idFonteDados = dfNomFonteDados["idFonteDados"].to_string(index=False)

        registra_log(
            arquivo,
            nomeProjeto,
            nomeFonteDado,
            nomEtapa,
            "S",
            f"{indice} - VALIDAÇÃO DE FONTE DE DADO CADASTRADA"
        )

    except pc.IntegrityError as e:

        registra_log(
            arquivo,
            nomeProjeto,
            nomeFonteDado,
            nomEtapa,
            "E",
            f"{indice} - ERROR: INTEGRITY CONSTRAINT VIOLATION OCCURRED: "+ str(e)
        )

    except pc.Error as e:

        registra_log(
            arquivo,
            nomeProjeto,
            nomeFonteDado,
            nomEtapa,
            "E",
            f"{indice} - ERROR: FAILED TO INSERT DATA INTO THE DATABASE: "+ str(e)
        )

    except NameError as e:

        registra_log(
            arquivo,
            nomeProjeto,
            nomeFonteDado,
            nomEtapa,
            "E",
            f"{indice} - ERROR: NAME IS NOT DEFINED: " + str(e)
        )

    finally:

        cur.close()
        cnxn.close()

    indice += 1

    return idFonteDados, indice


#####################################################################################################################
# Número da Regra: 1
# Breve Descrição: Etapa destinada para criação de função Python responsável pelo registro dos logs de processamento
# Data Criação: 09/04/2024
# Criado por: Gabriel Quintella
#####################################################################################################################

def importa_projeto(arquivo, aba, nomEtapa, indice=1):

    try:

        df = pd.read_excel(
            arquivo,
            sheet_name=aba,
            names=[
                "DscUltimaAcao",
                "IdProjeto",
                "NomCategoriaProjeto",
                "NomProjeto",
                "NumMaxIteracoes",
                "NomRespProjeto",
                "NomRespTecnico",
            ],
            dtype={
                "DscUltimaAcao": "str",
                "IdProjeto": "str",
                "NomCategoriaProjeto": "str",
                "NomProjeto": "str",
                "NumMaxIteracoes": "str",
                "NomRespProjeto": "str",
                "NomRespTecnico": "str",
            },
            header=0,
        )

        cnxn = conexao_db()
        cur = cnxn.cursor()

        nomeProjeto = df["NomProjeto"].to_string(index=False)
        tpoAcao = df["DscUltimaAcao"].to_string(index=False)

        idProjeto, indice = consulta_projeto(
                                    arquivo
                                    , nomeProjeto
                                    , "ETAPA DE CONSULTA DE PROJETO."
                                    , indice
                                    )

        if tpoAcao == 'CADASTRAR':

            if idProjeto != -1:

                registra_log(
                    arquivo,
                    nomeProjeto,
                    nomeProjeto,
                    f"ETAPA DE CONSULTA DE PROJETO ANTES DE CADASTRAR.",
                    "S",
                    f"{indice} - CADASTRO NÃO REALIZADO POIS O PROJETO JÁ ESTÁ CADASTRADO."
                )

            elif idProjeto == -1:

                for index, rowProjeto in df.iterrows():

                    cur.execute(
                        """INSERT INTO dq.TB_PROJETO
                                (
                                NomCategoriaProjeto
                                ,NomProjeto
                                ,NumMaxIteracoes
                                ,NomRespProjeto
                                ,NomRespTecnico
                                ,DscUltimaAcao
                                )
                                values(?,?,?,?,?,?)""",
                                rowProjeto.NomCategoriaProjeto,
                                rowProjeto.NomProjeto,
                                rowProjeto.NumMaxIteracoes,
                                rowProjeto.NomRespProjeto,
                                rowProjeto.NomRespTecnico,
                                rowProjeto.DscUltimaAcao
                    )

                    registra_log(
                        arquivo,
                        nomeProjeto,
                        nomeProjeto,
                        nomEtapa,
                        "S",
                        f"{indice} - PROJETO CADASTRADO COM SUCESSO."
                    )

            cnxn.commit()
            cur.close()

            idProjeto , indice = consulta_projeto(arquivo
                                         , nomeProjeto
                                         , "RETORNO DO CÓDIGO DO PROJETO CADASTRADO"
                                         , indice)

        elif tpoAcao == 'ALTERAR':

            print('exclusao')

        elif tpoAcao == 'EXCLUIR':

            print('exclusao')

        else:
           
            print('Erro de Preenchimento')

    except pc.IntegrityError as e:

        registra_log(
            arquivo,
            nomeProjeto,
            nomeProjeto,
            nomEtapa,
            "E",
            f"{indice} - ERROR: INTEGRITY CONSTRAINT VIOLATION OCCURRED:" + str(e)
        )

    except pc.Error as e:

        registra_log(
            arquivo,
            nomeProjeto,
            nomeProjeto,
            nomEtapa,
            "E",
            f"{indice} - ERROR: FAILED TO INSERT DATA INTO THE DATABASE: " + str(e)
        )

    except Exception as e:
        
        registra_log(
            arquivo,
            nomeProjeto,
            nomeProjeto,
            nomEtapa,
            "E",
            f"{indice} - OCORREU UM ERRO DURANTE O PROCESSO DE IMPORTAÇÃO DE PROJETOS. ERROR: " + str(e)
        )

    return idProjeto, nomeProjeto, indice


#####################################################################################################################
# Número da Regra: 1
# Breve Descrição: Etapa destinada para criação de função Python responsável pela importação da fonte de dado
# Data Criação: 09/04/2024
# Criado por: Gabriel Quintella
#####################################################################################################################

def importa_fonte_dado(arquivo, nomeProjeto, aba, nomEtapa, indice):

    try:

        df = pd.read_excel(
            arquivo,
            sheet_name=aba,
            names=[
                "DscUltimaAcao",
                "IdProjeto",
                "NomFonteDados",
                "DscFonteDados",
                "DataLakeFolder",
                "DscExtensaoArquivo",
                "TpoDelimitadorArquivo",
                "NomTabela",
                "NomAbaExcel",
                "NomEncodingArquivo",
            ],
            dtype={
                "DscUltimaAcao": "str",
                "IdProjeto": "str",
                "NomFonteDados": "str",
                "DscFonteDados": "str",
                "DataLakeFolder": "str",
                "DscExtensaoArquivo": "str",
                "TpoDelimitadorArquivo": "str",
                "NomTabela": "str",
                "NomAbaExcel": "str",
                "NomEncodingArquivo": "str",
            },
            header=0,
        )

        cnxn = conexao_db()
        cur = cnxn.cursor()

        nomeFonteDados = df["NomFonteDados"].to_string(index=False)

        tpoAcao = df["DscUltimaAcao"].to_string(index=False)

        if tpoAcao == 'CADASTRAR':

            idProjeto, indice = consulta_projeto(arquivo
                                , nomeProjeto
                                , nomEtapa
                                , indice)

            idFonteDados, indice = consulta_fonte_dado(arquivo
                                            , idProjeto
                                            , nomeFonteDados
                                            , nomEtapa
                                            , indice)

            df["IdProjeto"] = idProjeto

            if idFonteDados == 1:

                registra_log(
                    arquivo,
                    nomeProjeto,
                    nomeFonteDados,
                    nomEtapa,
                    "S",
                    f"{indice} - FONTE DE DADOS JÁ ESTÁ CADASTRADO NA BASE DE DADOS."
                )

            elif idFonteDados == -1:

                for index, rowFonteDado in df.iterrows():

                    topArquivo = rowFonteDado.DscExtensaoArquivo

                    if topArquivo == 'xlsx' or topArquivo == 'XLSX':

                        v_aba = rowFonteDado.NomAbaExcel
                        v_delimitador = ''
                        v_encoding = ''

                    else:

                        v_aba = ''
                        v_delimitador = rowFonteDado.TpoDelimitadorArquivo
                        v_encoding = rowFonteDado.NomEncodingArquivo
                
                    cur.execute(
                        """INSERT INTO [dq].[TB_FONTE_DADO]
                            (
                                [IdProjeto]
                                ,[NomFonteDados]
                                ,[DscFonteDados]
                                ,[DataLakeFolder]
                                ,[DscExtensaoArquivo]
                                ,[TpoDelimitadorArquivo]
                                ,[NomTabela]
                                ,[NomAbaExcel]
                                ,[NomEncodingArquivo]
                                ,[DscUltimaAcao]
                            ) values(?,?,?,?,?,?,?,?,?,?)""",
                        rowFonteDado.IdProjeto,
                        rowFonteDado.NomFonteDados,
                        rowFonteDado.DscFonteDados,
                        rowFonteDado.DataLakeFolder,
                        rowFonteDado.DscExtensaoArquivo,
                        v_delimitador,
                        rowFonteDado.NomTabela,
                        v_aba,
                        v_encoding,
                        rowFonteDado.DscUltimaAcao
                    )
                    
                    cnxn.commit()
                    cur.close()

                    if cur.rowcount > 0:

                        registra_log(
                            arquivo,
                            nomeProjeto,
                            nomeFonteDados,
                            nomEtapa,
                            "S",
                            f"{indice} - FONTE DE DADOS CADASTRADO COM SUCESSO."
                        )

                    idFonteDados, indice = consulta_fonte_dado(arquivo
                                                    , idProjeto
                                                    , nomeFonteDados
                                                    , nomEtapa
                                                    , indice)

        elif tpoAcao == 'ALTERAR':

            atualiza_fonte_dado(arquivo
                                , nomeProjeto
                                , aba
                                , nomEtapa
                                , indice)

        elif tpoAcao == 'EXCLUIR':

            exclui_fonte_dado(arquivo
                              , aba
                              , nomEtapa
                              , indice)      

    except pc.IntegrityError as e:

        registra_log(
            arquivo,
            nomeProjeto,
            nomeFonteDados,
            nomEtapa,
            "E",
            f"{indice} - ERROR: INTEGRITY CONSTRAINT VIOLATION OCCURRED:" + str(e)
        )

    except pc.Error as e:

        registra_log(
            arquivo,
            nomeProjeto,
            nomeFonteDados,
            nomEtapa,
            "E",
            f"{indice} - ERROR: FAILED TO INSERT DATA INTO THE DATABASE:" + str(e)
        )

    except NameError as e:

        registra_log(
            arquivo,
            nomeProjeto,
            nomeFonteDados,
            nomEtapa,
            "E",
            f"{indice} - ERROR: NAME IS NOT DEFINED:" + str(e)
        )

    except Exception as e:

        registra_log(
            arquivo,
            nomeProjeto,
            nomeFonteDados,
            nomEtapa,
            "E",
            f"{indice} - OCORREU UM ERRO DURANTE O PROCESSO DE IMPORTAÇÃO DA FONTE DE DADOS. ERROR: " + str(e)
        )

    return idFonteDados, indice


#####################################################################################################################
# Número da Regra: 1
# Breve Descrição: Etapa destinada para criação de função Python responsável pela atualização de uma fonte de dado cadastrada
# Data Criação: 09/04/2024
# Criado por: Gabriel Quintella
#####################################################################################################################

def atualiza_fonte_dado(arquivo, nomeProjeto, aba, nomEtapa, indice):

    try:

        df = pd.read_excel(
            arquivo,
            sheet_name=aba,
            names=[
                "DscUltimaAcao",
                "IdProjeto",
                "NomFonteDados",
                "DscFonteDados",
                "DataLakeFolder",
                "DscExtensaoArquivo",
                "TpoDelimitadorArquivo",
                "NomTabela",
                "NomAbaExcel",
                "NomEncodingArquivo",
            ],
            dtype={
                "DscUltimaAcao": "str",
                "IdProjeto": "str",
                "NomFonteDados": "str",
                "DscFonteDados": "str",
                "DataLakeFolder": "str",
                "DscExtensaoArquivo": "str",
                "TpoDelimitadorArquivo": "str",
                "NomTabela": "str",
                "NomAbaExcel": "str",
                "NomEncodingArquivo": "str",
            },
            header=0,
        )        

        cnxn = conexao_db()
        cur = cnxn.cursor()

        nomeFonteDados = df['NomFonteDados'].to_string(index=False)

        df_alteracao = df[df['DscUltimaAcao']=='ALTERAR']

        for index, rowFonteDado in df_alteracao.iterrows():

            v_delimitador = ''
            v_aba = ''
            v_encoding = ''

            cur.execute("""UPDATE [dq].[TB_FONTE_DADO]
                            SET [NomFonteDados] = ?
                                ,[DscFonteDados] = ?
                                ,[DataLakeFolder] = ?
                                ,[DscExtensaoArquivo] = ?
                                ,[TpoDelimitadorArquivo] = ?
                                ,[NomTabela] = ?
                                ,[NomAbaExcel] = ?
                                ,[NomEncodingArquivo] = ?
                                ,[DscUltimaAcao] = ? 
                                ,[DthAlteracao] = GETDATE()                               
                            WHERE [IdFonteDados] = ?;""",
                            rowFonteDado.NomFonteDados,
                            rowFonteDado.DscFonteDados,
                            rowFonteDado.DataLakeFolder,
                            rowFonteDado.DscExtensaoArquivo,
                            v_delimitador,
                            rowFonteDado.NomTabela,
                            v_aba,
                            v_encoding,
                            rowFonteDado.DscUltimaAcao,
                            rowFonteDado.IdProjeto
                        )
            
            cnxn.commit()
            cur.close()

            registra_log(
                arquivo,
                nomeProjeto,
                nomeFonteDados,
                nomEtapa,
                "E",
                f"{indice} - FONTE DE DADOS ATUALIZADA COM SUCESSO"
            )            

    except Exception as e:

        registra_log(
            arquivo,
            nomeProjeto,
            nomeFonteDados,
            nomEtapa,
            "E",
            f"{indice} - OCORREU UM ERRO DURANTE O PROCESSO DE ATUALIZAÇÃO DA FONTE DE DADOS. ERROR: " + str(e)
        )

#####################################################################################################################
# Número da Regra: 1
# Breve Descrição: 	Etapa destinada para criação de função Python responsável pela exclusão de uma fonte de dado cadastrada
# Data Criação: 09/04/2024
# Criado por: Gabriel Quintella
#####################################################################################################################

def exclui_fonte_dado(arquivo, aba, nomEtapa, indice):

    try:

        df = pd.read_excel(
            arquivo,
            sheet_name=aba,
            names=[
                "DscUltimaAcao",
                "IdFonteDados",
                "NomFonteDados",
                "DscFonteDados",
                "DataLakeFolder",
                "DscExtensaoArquivo",
                "TpoDelimitadorArquivo",
                "NomTabela",
                "NomAbaExcel",
                "NomEncodingArquivo",
            ],
            dtype={
                "DscUltimaAcao": "str",
                "IdFonteDados": "str",
                "NomFonteDados": "str",
                "DscFonteDados": "str",
                "DataLakeFolder": "str",
                "DscExtensaoArquivo": "str",
                "TpoDelimitadorArquivo": "str",
                "NomTabela": "str",
                "NomAbaExcel": "str",
                "NomEncodingArquivo": "str",
            },
            header=0,
        )        

        cnxn = conexao_db()
        cur = cnxn.cursor()

        nomeFonteDados = df['NomFonteDados'].to_string(index=False)

        queryIdProjeto = f"""SELECT NomProjeto
                    FROM [dq].[TB_PROJETO] A
                    INNER JOIN [dq].[TB_FONTE_DADO] B
                    ON A.IdProjeto = B.IdProjeto
                    WHERE [NomFonteDados] = '{nomeFonteDados}';"""

        dfNomeProjeto = pd.read_sql(queryIdProjeto, cnxn)      

        nomeProjeto = dfNomeProjeto['NomProjeto'].to_string(index=False)  

        df_exclusao = df[df['DscUltimaAcao']=='EXCLUIR']  

        for index, rowFonteDado in df_exclusao.iterrows():

            cur.execute(
                            """DELETE FROM [dq].[TB_ESTRUTURA_FONTE_DADO] WHERE [IdFonteDados] = ?;
                                DELETE FROM [dq].[TB_FONTE_DADO] WHERE [IdFonteDados] = ?;""",
                            rowFonteDado.IdFonteDados,
                            rowFonteDado.IdFonteDados
                        )
            
            cnxn.commit()
            cur.close()

            registra_log(
                arquivo,
                nomeProjeto,
                nomeFonteDados,
                nomEtapa,
                "E",
                f"{indice} - FONTE DE DADOS EXCLUIDA COM SUCESSO."
            )            

    except Exception as e: 

        registra_log(
            arquivo,
            nomeProjeto,
            nomeFonteDados,
            nomEtapa,
            "E",
            f"{indice} - OCORREU UM ERRO DURANTE O PROCESSO DE EXCLUSÃO DA FONTE DE DADOS. ERROR: " + str(e)
        )

#####################################################################################################################
# Número da Regra: 1
# Breve Descrição: Etapa destinada para criação de função Python responsável pela importação da estrutura de uma fonte de dado
# Data Criação: 09/04/2024
# Criado por: Gabriel Quintella
#####################################################################################################################

def importa_estrutura_fonte_dado(arquivo, aba, idProjeto, idFonteDado, IdEstruturaFonteDados, nomEtapa, indice):

    try:

        df = pd.read_excel(
            arquivo,
            sheet_name=aba,
            names=[
                "DscUltimaAcao",
                "IdProjeto",
                "IdFonteDados",
                "NomFonteDados",                        
                "NumSeqColunaTabela",            
                "NomColuna",
                "TpoDado",
                "FlagColunaNula",
                "FlagColunaObrigatoria",
                "FlagColunaChave",
                "TpoDadoArmazenado",
            ],
            dtype={
                "DscUltimaAcao": "str",
                "IdProjeto": "str",
                "IdFonteDados": "str",
                "NomFonteDados": "str",              
                "NumSeqColunaTabela": "str",            
                "NomColuna": "str",
                "TpoDado": "str",
                "FlagColunaNula": "str",
                "FlagColunaNula": "str",
                "FlagColunaObrigatoria": "str",
                "FlagColunaChave": "str",
                "TpoDadoArmazenado": "str"
            },
            header=0,
        )

        cnxn = conexao_db()
        cur = cnxn.cursor()

        v_NomFonteDados = df["NomFonteDados"].drop_duplicates(keep="last").head(1).values[0]

        tpoAcao = df["DscUltimaAcao"].drop_duplicates(keep="last").head(1).values[0]

        queryNomProjeto = f"""SELECT P.IdProjeto, FD.IdFonteDados, P.NomProjeto, FD.NomFonteDados
                                FROM dq.TB_PROJETO P
                                INNER JOIN dq.TB_FONTE_DADO FD
                                ON P.IdProjeto = FD.IdProjeto
                                WHERE FD.[NomFonteDados] = '{v_NomFonteDados}';"""
        
        dfNomProjeto = pd.read_sql(queryNomProjeto, cnxn)

        v_NomeProjeto = dfNomProjeto["NomProjeto"].to_string(index=False)
        v_FonteDados = dfNomProjeto["NomFonteDados"].to_string(index=False)

        if tpoAcao == 'CADASTRAR':

            if IdEstruturaFonteDados != "0":

                registra_log(
                    arquivo,
                    v_NomeProjeto,
                    v_FonteDados,
                    nomEtapa,
                    "S",
                    f"{indice} - A ESTRUTURA DO ARQUIVO JÁ ESTÁ CADASTRADO EM NOSSA BASE DE DADOS!"
                )

            elif IdEstruturaFonteDados == "0":

                for index, rowEstruturaFonteDado in df.iterrows():

                    queryFD = f"""INSERT INTO [dq].[TB_ESTRUTURA_FONTE_DADO](
                                    [DscUltimaAcao]
                                    ,[IdProjeto]
                                    ,[IdFonteDados]
                                    ,[NumSeqColunaTabela]                                 
                                    ,[NomColuna]
                                    ,[TpoDado]
                                    ,[FlagColunaNula]
                                    ,[FlagColunaObrigatoria]
                                    ,[FlagColunaChave]
                                    ,[TpoDadoArmazenado]                                 
                                    ) 
                                    values(
                                        '{rowEstruturaFonteDado.DscUltimaAcao}'
                                        ,{idProjeto}
                                        ,{idFonteDado}
                                        ,{rowEstruturaFonteDado.NumSeqColunaTabela}
                                        ,'{rowEstruturaFonteDado.NomColuna}'
                                        ,'{rowEstruturaFonteDado.TpoDado}'
                                        ,'{rowEstruturaFonteDado.FlagColunaNula}'
                                        ,'{rowEstruturaFonteDado.FlagColunaObrigatoria}'
                                        ,'{rowEstruturaFonteDado.FlagColunaChave}'
                                        ,'{rowEstruturaFonteDado.TpoDadoArmazenado}'
                                        )"""

                    cur.execute(queryFD)

                    registra_log(
                        arquivo,
                        v_NomeProjeto,
                        v_FonteDados,
                        nomEtapa,
                        "S",
                        f"{indice} - PROJETO CADASTRADO COM SUCESSO."
                    )

                cnxn.commit()
                cur.close()

                IdEstruturaFonteDados = consulta_estrutura_fonte_dado(arquivo
                                                                        , aba
                                                                        , idProjeto
                                                                        , idFonteDado
                                                                        , "ETAPA DE CONSULTA DA ESTRUTURA CADASTRADA."
                                                                        , indice)

        elif tpoAcao == 'ALTERAR':

            atualiza_estrutura_fonte_dado(arquivo
                                          , aba
                                          , f"{indice} - ETAPA DE ATUALIZAÇÃO DE FONTE DE DADO."
                                          , indice) 

        else:
           
            print('Erro de Preenchimento')        

    except pc.IntegrityError as e:

        registra_log(
            arquivo,
            v_NomeProjeto,
            v_FonteDados,
            nomEtapa,
            "E",
            f"{indice} - ERROR: INTEGRITY CONSTRAINT VIOLATION OCCURRED: "+ str(e)
        )

    except pc.Error as e:

        registra_log(
            arquivo,
            v_NomeProjeto,
            v_FonteDados,
            nomEtapa,
            "E",
            f"{indice} - ERROR: FAILED TO INSERT DATA INTO THE DATABASE: " + str(e)
        )

    except Exception as e:

        registra_log(
            arquivo,
            v_NomeProjeto,
            v_FonteDados,
            nomEtapa,
            "E",
            f"{indice} - OCORREU UM ERRO: " + str(e)
        )

    return IdEstruturaFonteDados

#####################################################################################################################
# Número da Regra: 1
# Breve Descrição: 	Etapa destinada para criação de função Python responsável pela consulta da estrutura de uma fonte de dado antes de cadastrar
# Data Criação: 09/04/2024
# Criado por: Gabriel Quintella
#####################################################################################################################

def consulta_estrutura_fonte_dado(arquivo, abaEstruturaFonteDado, idProjeto, idFonteDado, nomEtapa, indice):

    try:

        df = pd.read_excel(arquivo, sheet_name=abaEstruturaFonteDado)

        cnxn = conexao_db()
        cur = cnxn.cursor()

        queryNomProjeto = f"""SELECT ISNULL(b.NomProjeto,'-1') NomProjeto
                        FROM [dq].[TB_PROJETO] B
                        WHERE B.[IdProjeto] = {idProjeto};"""

        dfNomProjeto = pd.read_sql(queryNomProjeto, cnxn)

        if dfNomProjeto.empty:

            idProjeto = -1

            IdEstruturaFonteDados = -1

            registra_log(
                arquivo,
                idProjeto,
                idFonteDado,
                f"{indice} - ETAPA DE CONSULTA DE PROJETO.",
                "S",
                f"{indice} - PROJETO JÁ ESTÁ CADASTRADO NA BASE DE DATA QUALITY."
            )

        else:
            
            queryIdProjeto = f"""SELECT ISNULL(max(A.IdEstruturaFonteDados),0) IdEstruturaFonteDados, C.[NomFonteDados]
                                FROM [dq].[TB_ESTRUTURA_FONTE_DADO] A 
                                INNER JOIN [dq].[TB_PROJETO] B
                                on A.[IdProjeto] = B.[IdProjeto]
                                INNER JOIN [dq].[TB_FONTE_DADO] C
                                on A.[IdProjeto] = C.[IdProjeto]
                                WHERE a.[IdProjeto] = {idProjeto} 
                                AND a.[IdFonteDados] = {idFonteDado}
                                GROUP BY C.[NomFonteDados];"""
            
            dfFonteDados = pd.read_sql(queryIdProjeto, cnxn)

            IdEstruturaFonteDados = dfFonteDados["IdEstruturaFonteDados"].to_string(index=False)

            nomeProjeto = dfNomProjeto["NomProjeto"].to_string(index=False)

            nomeFonteDados = dfFonteDados["NomFonteDados"].to_string(index=False)            

            if dfFonteDados.empty:

                IdEstruturaFonteDados='0'

                importa_estrutura_fonte_dado(
                    arquivo,
                    abaEstruturaFonteDado,
                    idProjeto,
                    idFonteDado,
                    IdEstruturaFonteDados,
                    nomEtapa,
                    indice
                )               

            else:

                registra_log(
                    arquivo,
                    nomeProjeto,
                    nomeFonteDados,
                    "ETAPA DE CONSULTA DA FONTE DE DADO.",
                    "S",
                    f"{indice} - FOI ENCONTRADA UMA FONTE DE DADO CADASTRADO NA BASE DE DATA QUALITY."
                )                 

    except pc.IntegrityError as e:

        registra_log(
            arquivo,
            nomeProjeto,
            nomeFonteDados,
            nomEtapa,
            "E",
            f"{indice} - ERROR: INTEGRITY CONSTRAINT VIOLATION OCCURRED:" + str(e)
        )

    except pc.Error as e:

        registra_log(
            arquivo,
            nomeProjeto,
            nomeFonteDados,
            nomEtapa,
            "E",
            f"{indice} - ERROR: FAILED TO INSERT DATA INTO THE DATABASE:" + str(e)
        )

    else:

        if idProjeto == -1:

            registra_log(
                arquivo,
                nomeProjeto,
                nomeFonteDados,
                nomEtapa,
                "E",
                f"{indice} - NÃO EXISTE UM PROJETO CADASTRADO PARA ESSA ESTRUTURA."
            )

        else:
        
            registra_log(
                arquivo,
                nomeProjeto,
                nomeFonteDados,
                nomEtapa,
                "E",
                f"{indice} - EXISTE UM PROJETO CADASTRADO PARA ESSA ESTRUTURA."
            )

    finally:

        cur.close()
        cnxn.close()

    indice +=1

    return IdEstruturaFonteDados, indice

#####################################################################################################################
# Número da Regra: 1
# Breve Descrição: Etapa destinada para criação de função Python responsável pela atualização da estrutura de uma fonte de dado
# Data Criação: 09/04/2024
# Criado por: Gabriel Quintella
#####################################################################################################################

def atualiza_estrutura_fonte_dado(arquivo, aba, nomEtapa, indice):

    try:

        df = pd.read_excel(
            arquivo,
            sheet_name=aba,
            names=[
                "DscUltimaAcao",
                "IdProjeto",
                "IdFonteDados",
                "NomFonteDados",                        
                "NumSeqColunaTabela",            
                "NomColuna",
                "TpoDado",
                "FlagColunaNula",
                "FlagColunaObrigatoria",
                "FlagColunaChave",
                "TpoDadoArmazenado",
            ],
            dtype={
                "DscUltimaAcao": "str",
                "IdProjeto": "str",
                "IdFonteDados": "str",
                "NomFonteDados": "str",              
                "NumSeqColunaTabela": "str",            
                "NomColuna": "str",
                "TpoDado": "str",
                "FlagColunaNula": "str",
                "FlagColunaNula": "str",
                "FlagColunaObrigatoria": "str",
                "FlagColunaChave": "str",
                "TpoDadoArmazenado": "str"
            },
            header=0,
        )

        cnxn = conexao_db()
        cur = cnxn.cursor()

        nomeFonteDados = df['NomFonteDados'].to_string(index=False)

        queryIdProjeto = f"""SELECT NomProjeto
                    FROM [dq].[TB_PROJETO] A
                    INNER JOIN [dq].[TB_FONTE_DADO] B
                    ON A.IdProjeto = B.IdProjeto
                    WHERE [NomFonteDados] = '{nomeFonteDados}';"""

        dfNomeProjeto = pd.read_sql(queryIdProjeto, cnxn)      

        nomeProjeto = dfNomeProjeto['NomProjeto'].to_string(index=False)          
        
        df_EstruturaFonteDado = df[df['DscUltimaAcao'] == 'ALTERAR'][['NomColuna'
                                                                        ,'TpoDado'
                                                                        ,'FlagColunaNula'
                                                                        ,'FlagColunaObrigatoria'
                                                                        ,'FlagColunaChave'
                                                                        ,'TpoDadoArmazenado'
                                                                        ,'DscUltimaAcao'
                                                                        ,'IdFonteDados'
                                                                        ,'IdProjeto'
                                                                        ,'NumSeqColunaTabela']]


        for index, rowFonteDado in df_EstruturaFonteDado.iterrows():

            cur.execute("""UPDATE [dq].[TB_ESTRUTURA_FONTE_DADO]
                                SET [NomColuna] = ?
                                    ,[TpoDado] = ?
                                    ,[FlagColunaNula] = ?
                                    ,[FlagColunaObrigatoria] = ?
                                    ,[FlagColunaChave] =?
                                    ,[TpoDadoArmazenado] =?
                                    ,[DscUltimaAcao] = ?
                                    ,[DthAlteracao] = GETDATE()                               
                                WHERE [IdFonteDados] = ? 
                                AND [IdProjeto] = ?
                                AND [NumSeqColunaTabela] = ?;""",
                            rowFonteDado.NomColuna,
                            rowFonteDado.TpoDado,
                            rowFonteDado.FlagColunaNula,
                            rowFonteDado.FlagColunaObrigatoria,
                            rowFonteDado.FlagColunaChave,
                            rowFonteDado.TpoDadoArmazenado,
                            rowFonteDado.DscUltimaAcao,
                            rowFonteDado.IdFonteDados,
                            rowFonteDado.IdProjeto,
                            rowFonteDado.NumSeqColunaTabela
                        )
            
            cnxn.commit()
            cur.close()

            registra_log(
                arquivo,
                nomeProjeto,
                nomeFonteDados,
                nomEtapa,
                "E",
                f"{indice} - ESTRUTURA DA FONTE DE DADOS ATUALIZADA COM SUCESSO."
            )              

    except Exception as e:

        registra_log(
            arquivo,
            nomeProjeto,
            nomeFonteDados,
            nomEtapa,
            "E",
            f"{indice} - OCORREU UM ERRO DURANTE O PROCESSO DE ATUALIZAÇÃO DA ESTRUTURA DA FONTE DE DADOS. ERROR: " + str(e)
        )

#####################################################################################################################
# Número da Regra: 1
# Breve Descrição: Etapa destinada para criação de função Python responsável pela validação do arquivo de cadastro antes de iniciar o processo de importação.
# Data Criação: 09/04/2024
# Criado por: Gabriel Quintella
#####################################################################################################################

def valida_arquivo(arquivo, abaProjeto, abaFonteDado, abaEstruturaFonteDado):

    try: 

        ##################################################################################################
        # VARIÁVEIS USADAS NA VALIDAÇÃO DO ARQUIVO
        ##################################################################################################

        contador = 0
        msg = f'Prezado usuário, \n\nSegue o resumo do processo de validação do arquivo de cadastro de Data Quality.\n\n'
        listaAbas = ['Projeto', 'FonteDado', 'EstruturaFonteDado']
        listaEtapas = ['CADASTRAR','ALTERAR','EXCLUIR']
        listaBinaria = ['0','1']
        listaTipoDadoArmazenado = ['INTEIRO','NUMERIC','STRING','DATETIME','DATA','EMAIL']
        listatipoDatatype = ['INT','STRING','DATETIME','NUMERIC']

        ##################################################################################################
        # VALIDAÇÃO DA EXISTÊNCIA DAS ABAS DO ARQUIVO
        ##################################################################################################

        arquivo_excel = pd.ExcelFile(arquivo)

        listaAbaArquivo = arquivo_excel.sheet_names

        for v_aba in listaAbaArquivo:

            if v_aba not in listaAbas:

                contador += 1

                msg += f'* {contador} - Não foi encontrada a aba {v_aba} no arquivo {arquivo}. \n'

        ##################################################################################################
        # VALIDAÇÃO DA ABA PROJETO
        ##################################################################################################
                
        dfProjeto = pd.read_excel(
            arquivo,
            sheet_name=abaProjeto,
            names=[
                "DscUltimaAcao",
                "IdProjeto",
                "NomCategoriaProjeto",
                "NomProjeto",
                "NumMaxIteracoes",
                "NomRespProjeto",
                "NomRespTecnico",
            ],
            dtype={
                "DscUltimaAcao": "str",
                "IdProjeto": "str",
                "NomCategoriaProjeto": "str",
                "NomProjeto": "str",
                "NumMaxIteracoes": "str",
                "NomRespProjeto": "str",
                "NomRespTecnico": "str",
            },
            header=0,
        )

        totalRegistrosArquivo = dfProjeto.shape[0]      
        etapa = dfProjeto['DscUltimaAcao'].to_string(index=False)
        IdProjeto = dfProjeto['IdProjeto'].fillna(-99).to_string(index=False)
        categoriaProjeto = dfProjeto['NomCategoriaProjeto'].fillna(-99).to_string(index=False)
        nomeProjeto = dfProjeto['NomProjeto'].fillna(-99).to_string(index=False)
        numeroMaxIteracoes = dfProjeto['NumMaxIteracoes'].fillna(-99).to_string(index=False)
        nomeRespProjeto = dfProjeto['NomRespProjeto'].fillna(-99).to_string(index=False)
        nomeRespTecnico = dfProjeto['NomRespTecnico'].fillna(-99).to_string(index=False) 

        # VALIDA SE A ABA PROJETO ESTÁ PREENCHIDA
        if dfProjeto.empty:

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, a aba {v_aba} está sem informações. \n'

        if totalRegistrosArquivo > 1:

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {v_aba} foi identicado que existe mais de um registro. O que invalida o arquivo. \n'                           

        if (etapa == 'CADASTRAR' and IdProjeto != '-99'):

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaProjeto} foi identificado que para a Etapa de {etapa}, a coluna Código do Projeto não pode estar preenchida. \n'


        elif (((etapa == 'ALTERAR') or (etapa == 'EXCLUIR')) and (IdProjeto == '-99')):

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaProjeto} foi identificado que para a Etapa de {etapa}, a coluna Código do Projeto deve ser preenchida. \n'

        if (int(numeroMaxIteracoes) > 10):

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaProjeto} foi identificado que para a Etapa de {etapa}, a coluna Número de Execuções deverá estar entre 1 e 10. \n'


        if (categoriaProjeto == '-99'):

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaProjeto} foi identificado que para a Etapa de {etapa}, a coluna Categoria do Projeto está nula. \n'


        if (nomeProjeto == '-99'):

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaProjeto} foi identificado que para a Etapa de {etapa}, a coluna Nome do Projeto está nula. \n'


        if (nomeRespProjeto == '-99'):

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaProjeto} foi identificado que para a Etapa de {etapa}, a coluna Resposável pelo Projeto está nula. \n'


        if (nomeRespTecnico == '-99'):

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaProjeto} foi identificado que para a Etapa de {etapa}, a coluna Resposável Técnico pelo Projeto está nula. \n'


        if ('@' not in nomeRespProjeto):

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaProjeto} foi identificado que para a Etapa de {etapa}, a coluna Resposável pelo Projeto não possui um e-mail válido. \n'


        if ('@' not in nomeRespTecnico):

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaProjeto} foi identificado que para a Etapa de {etapa}, a coluna Resposável Técnico pelo Projeto não possui um e-mail válido. \n'

        ##################################################################################################
        # VALIDAÇÃO DA ABA FONTEDADO
        ##################################################################################################

        dfFonteDado = pd.read_excel(
            arquivo,
            sheet_name='FonteDado',
            names=[
                "DscUltimaAcao",
                "IdProjeto",
                "NomFonteDados",
                "DscFonteDados",
                "DataLakeFolder",
                "DscExtensaoArquivo",
                "TpoDelimitadorArquivo",
                "NomTabela",
                "NomAbaExcel",
                "NomEncodingArquivo",
            ],
            dtype={
                "DscUltimaAcao": "str",
                "IdProjeto": "str",
                "NomFonteDados": "str",
                "DscFonteDados": "str",
                "DataLakeFolder": "str",
                "DscExtensaoArquivo": "str",
                "TpoDelimitadorArquivo": "str",
                "NomTabela": "str",
                "NomAbaExcel": "str",
                "NomEncodingArquivo": "str",
            },
            header=0,
        )    

        etapa = dfFonteDado['DscUltimaAcao'].to_string(index=False)
        codioFonte = dfFonteDado['IdProjeto'].fillna(-99).to_string(index=False)
        nomeFonteDados = dfFonteDado['NomFonteDados'].fillna(-99).to_string(index=False)
        descricaoFonteDados = dfFonteDado['DscFonteDados'].fillna(-99).to_string(index=False)        
        dataLakeFolder = dfFonteDado['DataLakeFolder'].fillna(-99).to_string(index=False)
        extensaoArquivo = dfFonteDado['DscExtensaoArquivo'].fillna(-99).to_string(index=False)        
        tipoDelimitadorArquivo = dfFonteDado['TpoDelimitadorArquivo'].fillna(-99).to_string(index=False)        
        nomeTabela = dfFonteDado['NomTabela'].fillna(-99).to_string(index=False)        
        nomeAbaArquivoExcel = dfFonteDado['NomAbaExcel'].fillna(-99).to_string(index=False)                                
        encodingArquivo = dfFonteDado['NomEncodingArquivo'].fillna(-99).to_string(index=False)                     

        if dfFonteDado.empty:

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, a aba {v_aba} está sem informações. \n'            

        totalRegistrosArquivo = dfFonteDado.shape[0]     

        if totalRegistrosArquivo > 1:

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {v_aba} foi identicado que existe mais de um registro. O que invalida o arquivo. \n'            

        if (etapa == 'CADASTRAR' and codioFonte != '-99'):

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaFonteDado} foi identificado que para a Etapa de {etapa}, a coluna Código da Fonte não pode estar preenchida. \n'

        elif (((etapa == 'ALTERAR') or (etapa == 'EXCLUIR')) and (codioFonte == '-99')):

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaFonteDado} foi identificado que para a Etapa de {etapa}, a coluna Código da Fonte deve ser preenchida. \n'

        if (nomeFonteDados == '-99'):

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaFonteDado} foi identificado que para a Etapa de {etapa}, a coluna Nome da Fonte de Dado está nula. \n'

        if (descricaoFonteDados == '-99'):

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaFonteDado} foi identificado que para a Etapa de {etapa}, a coluna a descrição ou explicação da fonte de dados não pode ser nula. \n'

        if (dataLakeFolder == '-99'):

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaFonteDado} foi identificado que para a Etapa de {etapa}, a coluna referente ao local que fica o arquivo está nula. \n'

        if (extensaoArquivo == '-99'):

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaFonteDado} foi identificado que para a Etapa de {etapa}, a coluna referente extensão do arquivo está nula. \n'

        if ((extensaoArquivo == 'xlsx' or extensaoArquivo == 'XLSX')  and nomeAbaArquivoExcel == '-99'):

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaFonteDado} foi identificado que para a Etapa de {etapa}, a coluna referente extensão do arquivo é {extensaoArquivo} e a coluna referente a aba do arquivo está nula. \n'

        if ((extensaoArquivo in ['csv','CSV','txt','TXT'])  and nomeAbaArquivoExcel != '-99'):

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaFonteDado} foi identificado que para a Etapa de {etapa}, a coluna referente extensão do arquivo é {extensaoArquivo} e a coluna referente a aba do arquivo é {nomeAbaArquivoExcel}. Para os arquivos com a extensão {extensaoArquivo}, a coluna Aba do arquivo deverá ficar nula. \n'

        if ((extensaoArquivo in ['csv','CSV','txt','TXT'])  and tipoDelimitadorArquivo == '-99'):

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaFonteDado} foi identificado que para a Etapa de {etapa}, ao delimitador de coluna está nula. Para os arquivos com a extensão {extensaoArquivo}, a coluna delimitador deverá ser preenchida. \n'

        if ((extensaoArquivo in ['csv','CSV','txt','TXT'])  and encodingArquivo == '-99'):

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaFonteDado} foi identificado que para a Etapa de {etapa}, ao delimitador de coluna está nula. Para os arquivos com a extensão {extensaoArquivo}, a coluna encoding deverá ser preenchida. \n'

        if (nomeTabela == '-99'):

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaFonteDado} foi identificado que para a Etapa de {etapa}, a coluna refernete ao Nome da Tabela está nula. \n'

        ##################################################################################################
        # VALIDAÇÃO DA ABA ESTRUTURA FONTE DE DADO
        ##################################################################################################

        dfEstruturaFonteDado = pd.read_excel(
                    arquivo,
                    sheet_name=abaEstruturaFonteDado,
                    names=[
                        "DscUltimaAcao",
                        "IdProjeto",
                        "IdFonteDados",
                        "NomFonteDados",                        
                        "NumSeqColunaTabela",            
                        "NomColuna",
                        "TpoDado",
                        "FlagColunaNula",
                        "FlagColunaObrigatoria",
                        "FlagColunaChave",
                        "TpoDadoArmazenado",
                    ],
                    dtype={
                        "DscUltimaAcao": "str",
                        "IdProjeto": "str",
                        "IdFonteDados": "str",
                        "NomFonteDados": "str",              
                        "NumSeqColunaTabela": "str",            
                        "NomColuna": "str",
                        "TpoDado": "str",
                        "FlagColunaNula": "str",
                        "FlagColunaObrigatoria": "str",
                        "FlagColunaChave": "str",
                        "TpoDadoArmazenado": "str"
                    },
                    header=0,
                )

        etapa = dfEstruturaFonteDado['DscUltimaAcao'].to_string(index=False)
        codigoProjeto = dfEstruturaFonteDado['IdProjeto'].fillna(-99).to_list()
        codigoFonteDados = dfEstruturaFonteDado['IdFonteDados'].fillna(-99).to_list()
        nomeFonteDados = dfEstruturaFonteDado['NomFonteDados'].to_list()
        numSeqColunaTabela = dfEstruturaFonteDado['NumSeqColunaTabela'].fillna(-99).to_list()
        nomeColuna = dfEstruturaFonteDado['NomColuna'].to_list()
        tipoDado = dfEstruturaFonteDado['TpoDado'].fillna(-99).to_list()
        flagColunaNula = dfEstruturaFonteDado['FlagColunaNula'].to_list()
        flagColunaObrigatoria = dfEstruturaFonteDado['FlagColunaObrigatoria'].to_list()
        flagColunaChave = dfEstruturaFonteDado['FlagColunaChave'].to_list()
        tipoDadoArmazenado = dfEstruturaFonteDado['TpoDadoArmazenado'].to_list()

        totalRegistrosArquivo = dfEstruturaFonteDado.shape[0]  

        if dfEstruturaFonteDado.empty:

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, a aba {abaEstruturaFonteDado} está sem informações. \n'            



        if (etapa == 'CADASTRAR' and (codioFonte != '-99' or codigoFonteDados != '-99')):

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaEstruturaFonteDado} foi identificado que para a Etapa de {etapa}, a coluna Código da Fonte ou Código Fonte do Dado não pode estar preenchida. \n'

        elif (((etapa == 'ALTERAR') or (etapa == 'EXCLUIR')) and (codioFonte == '-99' or codigoFonteDados == '-99')):

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaEstruturaFonteDado} foi identificado que para a Etapa de {etapa}, a coluna Código da Fonte ou Código Fonte do Dado deve ser preenchida. \n'

        if codigoProjeto:

            valores_distintos = list(set(codigoProjeto))

            if len(valores_distintos) > 1:

                for seqProjeto, elemento in enumerate(valores_distintos):

                    contador += 1

                    msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaEstruturaFonteDado}, a coluna Código do Projeto os valores não são únicos. Valor encontrado: {elemento}. \n'

        for seq, elemento in enumerate(codigoProjeto):

            try:

                pd.to_numeric(elemento, downcast='integer', errors='raise')

            except ValueError as e:

                contador += 1

                msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaEstruturaFonteDado}, na linha {str(seq+1)} a coluna Código do Projeto possui um valor inválido. Valor encontrado: {str(elemento)}. \n' 


        if codigoFonteDados:

            valores_distintos_fd = list(set(codigoFonteDados))

            if len(valores_distintos_fd) > 1:

                for seqFonteDado, elemento in enumerate(valores_distintos_fd):

                    contador += 1

                    msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaEstruturaFonteDado}, a coluna Código Fonte Dado os valores não são únicos. Valor encontrado: {elemento}. \n'

        for seq, elemento in enumerate(codigoFonteDados):

            try:

                pd.to_numeric(elemento, downcast='integer', errors='raise')

            except ValueError as e:

                contador += 1

                msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaEstruturaFonteDado}, na linha {str(seq+1)} a coluna Código Fonte Dado possui um valor inválido. Valor encontrado: {str(elemento)}. \n' 
 
        tamanhoLista = [x for x in nomeFonteDados if pd.isnull(x) == False]

        if ( len(tamanhoLista) != totalRegistrosArquivo):

            contador += 1

            msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaFonteDado}, na coluna Nome da Fonte de Dado existe erro/ausência de preenchimento. \n'


        if nomeFonteDados:

            valores_distintos_nomeFonteDados = list(set(nomeFonteDados))

            if len(valores_distintos_nomeFonteDados) > 1:

                for seqnomeFonteDados, elemento in enumerate(valores_distintos_nomeFonteDados):

                    contador += 1

                    msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaEstruturaFonteDado}, a coluna Nome Fonte Dado os valores não são únicos. Valor encontrado: {elemento}. \n'

        for i, elemento in enumerate(nomeColuna):

            if pd.isna(elemento):
                
                contador += 1

                msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaEstruturaFonteDado}, na linha {str(i+1)} existe ausência de registro na coluna "Nome da Coluna". \n' 

        for seq, elemento in enumerate(numSeqColunaTabela):

            try:

                pd.to_numeric(elemento, downcast='integer', errors='raise')

            except ValueError as e:

                contador += 1

                msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaEstruturaFonteDado}, na linha {str(seq+1)} a coluna Sequência Coluna possui um valor inválido. Valor encontrado: {str(elemento)}. \n' 
        

        for seqColNula, colunaNula in enumerate(flagColunaNula):

            if pd.isna(colunaNula):
                
                contador += 1

                msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaEstruturaFonteDado}, na linha {str(seqColNula+1)} existe ausência de registro na coluna "Flag Coluna Nula". \n' 

            elif colunaNula not in listaBinaria:

                contador += 1

                msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaEstruturaFonteDado}, na linha {str(seqColNula+1)} existe um valor não permitido na coluna "Flag Coluna Nula". Valor Encontrado: {str(colunaNula)} \n' 

        for seqColObrigatoria, colunaObrigatoria in enumerate(flagColunaObrigatoria):

            if pd.isna(colunaObrigatoria):
                
                contador += 1

                msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaEstruturaFonteDado}, na linha {str(seqColObrigatoria+1)} existe ausência de registro na coluna "Flag Coluna Obrigatória". \n' 

            elif  colunaObrigatoria not in listaBinaria:

                contador += 1

                msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaEstruturaFonteDado}, na linha {str(seqColObrigatoria+1)} existe um valor não permitido na coluna "Flag Coluna Obrigatória". Valor Encontrado: {str(colunaObrigatoria)} \n' 

        for seqColChave, colunaChave in enumerate(flagColunaChave):

            if pd.isna(colunaChave):
                
                contador += 1

                msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaEstruturaFonteDado}, na linha {str(seqColChave+1)} existe ausência de registro na coluna "Flag Coluna Chave". \n' 

            elif colunaChave not in listaBinaria:

                contador += 1

                msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaEstruturaFonteDado}, na linha {str(seqColChave+1)} existe um valor não permitido na coluna "Flag Coluna Chave". Valor Encontrado: {str(colunaChave)} \n' 

        for seqColtipoDadoArmazenado, colunatipoDadoArmazenado in enumerate(tipoDadoArmazenado):

            if pd.isna(colunatipoDadoArmazenado):
                
                contador += 1

                msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaEstruturaFonteDado}, na linha {str(seqColtipoDadoArmazenado+1)} existe ausência de registro na coluna "Flag Tipo Dado Armazenado". \n' 

            elif colunatipoDadoArmazenado not in listaTipoDadoArmazenado:

                contador += 1

                msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaEstruturaFonteDado}, na linha {str(seqColtipoDadoArmazenado+1)} existe um valor não permitido na coluna "Flag Tipo Dado Armazenado". Valor Encontrado: {str(colunatipoDadoArmazenado)} \n' 


        for seqtpdado, tpdado in enumerate(tipoDado):

            if pd.isna(tpdado):
                
                contador += 1

                msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaEstruturaFonteDado}, na linha {str(seqtpdado+1)} existe ausência de registro na coluna "Tipo de Dado". \n' 

            elif tpdado not in listatipoDatatype:

                contador += 1

                msg += f'* {contador} - Durante a importação do arquivo {arquivo}, na aba {abaEstruturaFonteDado} na coluna TIPO DE DADO foi identificado um tipo de dado {tpdado} que não é permitido. \n'

        if contador == 0:

            msg += f'* {contador} - Não foi encontrada nenhuma inconsistência.'

        print(msg)

    except Exception as e:

        registra_log(
            arquivo,
            'N/A',
            'N/A',
            "ERRO NA EXECUÇÃO DA PIPELINE",
            "E",
            f"OCORREU UM ERRO: " + str(e)
        )  

    return contador, msg

#####################################################################################################################
# Número da Regra: 1
# Breve Descrição: Etapa principal do processo de importação do arquivo de cadastro da fonte no Data Quality.
# Número da Regra: 2
# Breve Descrição: Etapa destinada para criação de função Python responsável pela importação do arquivo.
# Data Criação: 09/04/2024
# Criado por: Gabriel Quintella
#####################################################################################################################

def importa_planilha_dq(arquivo, abaProjeto, abaFonteDado, abaEstruturaFonteDado):

    try:

        contador, msg = valida_arquivo(arquivo, abaProjeto, abaFonteDado, abaEstruturaFonteDado)

        if contador == 0:

            idProjeto, nomeProjeto, indice = importa_projeto(arquivo
                                                            , abaProjeto
                                                            , "ETAPA DE CADASTRO DE PROJETO"
                                                            )

            idFonteDados, indice = importa_fonte_dado(
                arquivo,
                nomeProjeto,
                abaFonteDado,
                "ETAPA DE CADASTRO DE FONTE DADOS",
                indice
            )

            IdEstruturaFonteDados, indice = consulta_estrutura_fonte_dado(
                arquivo,
                abaEstruturaFonteDado,
                idProjeto,
                idFonteDados,
                "ETAPA DE CONSULTA ESTRUTURA DA FONTE DE DADO",
                indice
            )

            registra_log(
                arquivo,
                nomeProjeto,
                nomeProjeto,
                "FINALIZAÇÃO NO PROCESSO DE CADASTRO NO DATA QUALITY",
                "E",
                f"{indice} - PROCESSO EXECUTADO COM SUCESSO."
            )  

            fromaddr = "dbaassists@gmail.com"
            toaddr = "dbaassists@gmail.com"
            assunto = "Data Quality - Inconsistências"

            envia_email(fromaddr, toaddr, assunto, msg)                

        else:

            print('envia email')     

            fromaddr = "dbaassists@gmail.com"
            toaddr = "dbaassists@gmail.com"
            assunto = "Data Quality - Inconsistências"

            envia_email(fromaddr, toaddr, assunto, msg)    

    except Exception as e:

        registra_log(
            arquivo,
            nomeProjeto,
            nomeProjeto,
            "ERRO NA EXECUÇÃO DA PIPELINE",
            "E",
            f"{indice} - OCORREU UM ERRO: " + str(e)
        )        