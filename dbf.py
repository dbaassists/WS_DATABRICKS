# %%

caminho = f'C:\Temp\dbf\BRIC657.DBF'

caminho2 = f'C:\Temp\dbf\PRIC657.DBF'

caminho3 = f'C:\Temp\dbf\PROD657.DBF'


# %%
from dbfread import DBF
import pandas as pd

# Abre o arquivo DBF
table = DBF(caminho3)

# Converte os registros para uma lista de dicionários
records = [record for record in table]

# Cria um DataFrame a partir da lista de registros
df = pd.DataFrame(records)

# %%

# Exibe o DataFrame
df
# %%


import os
caminho = fr'C:\Temp\dbf'
lista = os.listdir(caminho)

# %%

type(lista)

# %% 

for registros in lista:

    print(registros)

