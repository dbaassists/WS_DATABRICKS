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
