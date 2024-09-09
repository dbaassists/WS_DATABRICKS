#########################################################################
# Aula 02 - Estrutura de Dados
# Autor: Gabriel Quintella
# Data: 10/09/2024
# Assunto: 1. LISTA (List)
# Uma lista é uma estrutura de dados mutável que permite adicionar, remover e modificar seus elementos. 
# Onde uma lista permite armazenar objetos seja de um mesmo tipo ou não.
#########################################################################


# %%

# Criando uma lista

lista = []

# ou 

lista = list()

# %%

# Criando uma lista pré-definida 

lista = [10, 20, 30, 40]

# %%

# Identificando o tipo de um objeto

type(lista)

# %%

# Imprimindo o conteúdo de uma lista

print(lista)


# %%

# Adicionar um elemento à lista
# Nesse formato o elemento é sempre adicionado no final da fila

lista.append(50)


# %% 

# Navegando pelos elementos de uma lista por posições (indice)
# Consultar um elemento que não existe 

print(lista[0])

# %%

# Remover o elemento 20 da lista
lista.remove(20)

# %%

# Acessar o segundo elemento da lista (índice 1)

# %%

# Fatiamento (slicing)
# A posição 2 é exclusiva, ou seja, ela não entra

print(lista[0:2])


# %%

# Tudo até o elemento de posição 2
print(lista[:2])

# %%

# Retorna todos os elementos da lista
print(lista[:])


# %%

# Quando o indice for negativo, significa que estamos percorrendo a lista ao inverso

print(lista[-2])

# %%

# Retorna todos os registros até o penúltimo elemento da lista

print(lista[:-2])


# %%

# 

lista.append(50)


# %%
 
print(lista)



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

# Adicionando um elemento na lista e especificando em qual posição ele será inserido
lista = [10, 20, 30, 40]

lista.insert(2,50)

lista[2] = 50

print(lista)


# %%

# Removendo o último elemento da lista

lista = [10, 20, 30, 40]

lista.pop()

print(lista)

# %%

# Removendo elemento da lista informando o indice

lista = [10, 20, 30, 40]

lista.pop(2)

print(lista)


# %%

# Removendo elemento da lista pelo próprio elemento

lista = [10, 20, 30, 40]

lista.remove(30)

print(lista)


# %%

# Removendo elemento da lista pelo próprio elemento
# Obs: remove apenas a primeira ocorrência

lista = [10, 20, 30, 40, 50, 60, 20]

lista.remove(20)

print(lista)


# %%

# Apaga todos os elementos da lista

lista = [10, 20, 30, 40]

lista.clear()

print(lista)


# %%

# Removendo elemento da lista pelo próprio elemento
# Obs: remove apenas a primeira ocorrência

lista = [10, 20, 30, 40, 50, 60, 20]

lista.count(10)



# %%

# Removendo elementos duplicados
# Obs: não mantém a ordem

lista = [50,10, 30, 60, 20, 40, 30, 50, 20]

lista = list(set(lista))

print(lista)



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

lista.sort(reverse=True)

print(lista)


# %%

# Inverte a ordem dos elementos

lista = [50,10, 30, 60, 20, 40, 30, 50, 20]

lista.reverse()

print(lista)
