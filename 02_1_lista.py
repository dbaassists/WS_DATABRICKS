#########################################################################
# Aula 02 - Estrutura de Dados
# Autor: Gabriel Quintella
# Data: 10/09/2024
# Assunto: 1. LISTA (List)
# Uma lista é uma estrutura de dados mutável que permite adicionar, remover e modificar seus elementos. 
# Onde uma lista permite armazenar objetos seja de um mesmo tipo ou não.
#########################################################################

# %%

import os

diretorio = fr'C:\Temp\Python_YT\Git\MBA\01_Estrutura_Dados\02_Aula\Dados'

lista_arquivos_eletorais = os.listdir(diretorio)

type(lista_arquivos_eletorais)

len(lista_arquivos_eletorais)

print(lista_arquivos_eletorais)

if 'candidato' in lista_arquivos_eletorais:

    print(lista_arquivos_eletorais)

# %%

import os 
import shutil

diretorio = fr'C:\Temp\Python_YT\Git\MBA\01_Estrutura_Dados\02_Aula\Dados'

lista_arquivos_eletorais = os.listdir(diretorio)

for arquivo in lista_arquivos_eletorais:

    if arquivo.endswith('pdf'):

        os.makedirs(fr'C:\Temp\Python_YT\Git\MBA\01_Estrutura_Dados\02_Aula\Dados\Outros', exist_ok=True)
        shutil.move(diretorio + "\\" + arquivo, fr'C:\Temp\Python_YT\Git\MBA\01_Estrutura_Dados\02_Aula\Dados\Outros')

    elif 'consulta_vagas' in arquivo:

        os.makedirs(fr'C:\Temp\Python_YT\Git\MBA\01_Estrutura_Dados\02_Aula\Dados\Arquivo_Vagas', exist_ok=True)
        shutil.move(diretorio + "\\" + arquivo, fr'C:\Temp\Python_YT\Git\MBA\01_Estrutura_Dados\02_Aula\Dados\Arquivo_Vagas')


    elif 'consulta_coligacao' in arquivo:

        os.makedirs(fr'C:\Temp\Python_YT\Git\MBA\01_Estrutura_Dados\02_Aula\Dados\Arquivo_Coligacao', exist_ok=True)
        shutil.move(diretorio + "\\" + arquivo, fr'C:\Temp\Python_YT\Git\MBA\01_Estrutura_Dados\02_Aula\Dados\Arquivo_Coligacao')

    elif 'bem_candidato' in arquivo:

        os.makedirs(fr'C:\Temp\Python_YT\Git\MBA\01_Estrutura_Dados\02_Aula\Dados\Arquivo_Bem_Candidato', exist_ok=True)
        shutil.move(diretorio + "\\" + arquivo, fr'C:\Temp\Python_YT\Git\MBA\01_Estrutura_Dados\02_Aula\Dados\Arquivo_Bem_Candidato')

    elif 'consulta_cand' in arquivo:

        os.makedirs(fr'C:\Temp\Python_YT\Git\MBA\01_Estrutura_Dados\02_Aula\Dados\Arquivo_Candidato', exist_ok=True)
        shutil.move(diretorio + "\\" + arquivo, fr'C:\Temp\Python_YT\Git\MBA\01_Estrutura_Dados\02_Aula\Dados\Arquivo_Candidato')

# %%

import pyodbc
import pandas as pd

server = 'localhost\DBAASSISTS'
database = 'ERP_VENDAS' 
username = 'python' 
password = 'python' 
driver = 'SQL Server'

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

# Fechar a conexão
#conn.close()

# %%

df.info()

# %%

listaTabela = df['nomeTabela'].tolist()

# %% 


server = 'localhost\DBAASSISTS'
database = 'ERP_VENDAS' 
username = 'python' 
password = 'python' 
driver = 'SQL Server'

# Conectar ao SQL Server
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    fr'SERVER={server};'
    fr'DATABASE={database};'
    fr'UID={username};'
    fr'PWD={password}'
)

type(listaTabela)

len(listaTabela)

listaTabela

diretorio =  fr'C:\Temp\Python_YT\Git\MBA\01_Estrutura_Dados\02_Aula\Dados\Extracao_SQL_Server'

for tabela in listaTabela:

    print(tabela)

    df = pd.read_sql(fr"SELECT * FROM {tabela}", conn)

    # Criar o diretório, se ele não existir
    os.makedirs(diretorio, exist_ok=True)

    arquivoExtraido = diretorio + '\\' + tabela.replace('dbo.','').lower() + '.csv'

    df.to_csv(arquivoExtraido 
                , index=False
                , sep=';'
                , header= True)

# %%

# Criando uma lista vazia

lista = []

# ou 

# Criando uma lista vazia

lista = list()

# Criando uma definindo os valores

lista = [10, 20, 30, 40]

# %%

# Identificando o tipo de um objeto

type(lista)

# %%

# Verficando o conteúdo de uma lista

print(lista)

# %%

# Adicionar um elemento à lista
# Nesse formato o elemento é sempre adicionado no final da fila

lista.append(50)

# Adicionando um elemento na lista e especificando em qual posição ele será inserido
lista = [10, 20, 30, 40]

lista.insert(2,50)

lista[2] = 50

print(lista)

# %% 

# Navegando pelos elementos de uma lista por posições (indice)
# Consultar um elemento que não existe 

print(lista[0])

# %%

# Removendo o último elemento da lista
lista = [10, 20, 30, 40]
lista.pop()

# Removendo elemento da lista informando o indice
lista = [10, 20, 30, 40]
lista.pop(2)

# Remover o elemento 20 da lista
lista = [10, 20, 30, 40]
lista.remove(20)

# Removendo elemento da lista pelo próprio elemento
# Obs: remove apenas a primeira ocorrência
lista = [10, 20, 30, 40, 50, 60, 20]
lista.remove(20)

# Apaga todos os elementos da lista
lista = [10, 20, 30, 40]
lista.clear()
print(lista)


# %%

# Acessar o segundo elemento da lista (índice 1)

# %%

# Fatiamento (slicing)
# A posição 2 é exclusiva, ou seja, ela não entra
lista = [10, 20, 30, 40]
print(lista[0:2])

# %%

# Tudo até o elemento de posição 2
lista = [10, 20, 30, 40]
print(lista[:2])

# Tudo a partir do elemento de posição 2
lista = [10, 20, 30, 40]
print(lista[2:])

# %%

# Retorna todos os elementos da lista
lista = [10, 20, 30, 40]
print(lista[:])

# %%

# Quando o indice for negativo, significa que estamos percorrendo a lista ao inverso
lista = [10, 20, 30, 40]
print(lista[-2])

# %%

# Retorna todos os registros até o penúltimo elemento da lista
lista = [10, 20, 30, 40]
print(lista[:-2])

# %%

# Podemos percorrer uma lista com "for"
# Colocar um exemplo da vida real iterando com arquivos

# %%

# Alterar para montar como se fosse uma lista de cadastro de um aluno com dados e o último elemento é a nota dele

lista = [1, 2, 3, 4, 5, [6, 7, 8]]

print(lista[5])

print(lista[5][0])
print(lista[5][1])
print(lista[5][2])

# %% 

# Qual o tamanho de uma lista, quantos elementos existem?

len(lista)

# %% 

# Verificar a existência de um item em uma lista

# Operadores de Associação (in e not in) -> Eles são usados para verificar se um valor ou item está presente (ou ausente) 
# em uma sequência ou coleção, como strings, listas, tuplas, conjuntos ou dicionários.

valor = 'aluno'

lista = ['dados_aluno.csv','dados_professor.csv','pagamentos_aluno.csv','livros_biblioteca']

for i in lista:

    if valor in i:

        print("Existe aluno em: " + i)

    else:

        print("Não existe aluno em: " + i)

# %%

# Criando uma lista pré-definida 

lista = [10, 20, 30, 40]

max(lista)
min(lista)
sum(lista)

# %%

# Removendo elemento da lista pelo próprio elemento
# Obs: remove apenas a primeira ocorrência
lista = [10, 20, 30, 40, 50, 60, 20]
lista.count(20)

# %%

# Removendo elementos duplicados
# Obs: não mantém a ordem

lista = [50,10, 30, 60, 20, 40, 30, 50, 20]

lista = list(set(lista))

print(lista)

# %%

# Removendo elementos duplicados
# Obs: NÃO MANTÉM a ordem

# Um conjunto (set) é uma coleção não ordenada de elementos únicos

lista = [50,10, 30, 60, 20, 40, 30, 50, 20]

lista = list(set(lista))

lista.sort()

print(lista)

# %% 

import pandas as pd

lista = [50,10, 30, 60, 20, 40, 30, 50, 20]

df= pd.DataFrame(lista , columns=['ID']).sort_values(by='ID', ascending=False).drop_duplicates()

df

# %%

# Removendo elementos duplicados
# Obs: mantém a ordem

lista = [50,10, 30, 60, 20, 40, 30, 50, 20]

# Usando um loop para remover duplicatas e preservar a ordem

lista_sem_duplicatas = []

for item in lista:

    if item not in lista_sem_duplicatas:

        lista_sem_duplicatas.append(item)

print("Lista sem duplicatas (ordem preservada):", lista_sem_duplicatas)


# %%

# Removendo elementos duplicados
# Obs: mantém a ordem
# obs2: Python 3.7 e versões posteriores

lista = [50,10, 30, 60, 20, 40, 30, 50, 20]

# Remover duplicatas e preservar a ordem usando dict.fromkeys()
lista_sem_duplicatas = list(dict.fromkeys(lista))

print("Lista sem duplicatas (ordem preservada):", lista_sem_duplicatas)


# %%

# Ordenando a lista em forma crescente

lista = [50,10, 30, 60, 20, 40, 30, 50, 20]

lista.sort()
#lista.sort(reverse=False)

print(lista)

# %%

# Ordenando a lista em forma decrescente

lista = [50,10, 30, 60, 20, 40, 30, 50, 20]

lista.sort(reverse=False)

print(lista)

# %%

# Inverte a ordem dos elementos

lista = [50,10, 30, 60, 20, 40, 30, 50, 20]

lista.reverse()

print(lista)
