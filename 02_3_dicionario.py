# %%

carro = {}

type(carro)

# %%

carro = {'AZS-1234' : ['Gol',2024,'Gasolina']
,'ABH-1A33' : ['Pálio',2011,'Flex']
,'AOP-9329' : ['Voyage',2019,'Gasolina/Gás']
}

print(carro.keys())

for i in carro.keys():

    print(carro[i])
    
# %%

# Colocar um exemplo de um JSON que utilizo para extrair dados de um banco de dados

carro = {'AZS-1234' : ['Gol',2024,'Gasolina']
,'ABH-1A33' : ['Pálio',2011,'Flex']
,'AOP-9329' : ['Voyage',2019,'Gasolina/Gás']
}

print(carro.keys())

for i in carro.keys():

    print(carro[i])


# %% 

# Tratando um retorno sem um valor encontrado 

carro.get('ASD1234','Não encontrado')

# %%

# Tratando um retorno sem um valor encontrado 

carro.get('AZS-1234','Não encontrado')

# %%

# Tratando um retorno sem um valor encontrado 

'AZS-1234' in carro

# %%

'AZS-1234' not in carro

# %%

# Adicionando um registro em um dicionario
# Não funciona o append
carro.append('ASD-1234' , ['Duster', 2020,'Gasolina'])


# %%

carro['ASD-1234'] =  ['Duster', 2020,'Gasolina']

carro

# %% 

del carro['ASD-1234']

# %% 

carro.pop('ASD-1234', 'Nao Encontrado')

# %%

carro.clear()

print(carro)

# %% 

len(carro)

# %%

carro['ASD-1234'] =  ['Duster', 2021,'Gasolina']

carro

# %%

import json
import pyodbc
import pandas as pd

parametros = {"Sql_Server" : ["localhost\\DBAASSISTS","ERP_VENDAS","python","python","SQL Server"]}

print(parametros)

for dicionario in parametros.keys():

    server = parametros[dicionario][0]
    database = parametros[dicionario][1] 
    username = parametros[dicionario][2] 
    password = parametros[dicionario][3] 
    driver = parametros[dicionario][4]

    print(server)
    print(database) 
    print(username)
    print(password) 
    print(driver)    

    # Conectar ao SQL Server
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        fr'SERVER={server};'
        fr'DATABASE={database};'
        fr'UID={username};'
        fr'PWD={password}'
    )

    # Usar pandas para ler a tabela diretamente em um DataFrame
    df = pd.read_sql("""
                    SELECT s.name + '.' + t.name nomeTabela 
                    FROM sys.tables t
                    INNER JOIN sys.schemas s
                    ON t.schema_id = s.schema_id
    """, conn)

    # Exibir as primeiras linhas do DataFrame

    print(df.head(50))


# %%

import json
import pyodbc
import pandas as pd

arq_json = fr'C:\Temp\Python_YT\Git\MBA\01_Estrutura_Dados\02_Aula\parametro_db.json'

df =  pd.read_json(arq_json)

df

for param in df.keys():

    server = df[param][0]
    database = df[param][1] 
    username = df[param][2] 
    password = df[param][3] 
    driver = df[param][4]

    print(server)
    print(database) 
    print(username)
    print(password) 
    print(driver)

    # Conectar ao SQL Server
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        fr'SERVER={server};'
        fr'DATABASE={database};'
        fr'UID={username};'
        fr'PWD={password}'
    )

    # Usar pandas para ler a tabela diretamente em um DataFrame
    df = pd.read_sql("""
                    SELECT s.name + '.' + t.name nomeTabela 
                    FROM sys.tables t
                    INNER JOIN sys.schemas s
                    ON t.schema_id = s.schema_id
    """, conn)

    # Exibir as primeiras linhas do DataFrame

    print(df.head())

# %%


conn.close()