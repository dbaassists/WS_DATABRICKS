# %%

# São semelhantes as listas, porém imutáveis!
# Útil para armazenar valores que não mudam.
# Por ser imutável, caso precise incluir algum elemento, a tupla deverá ser recriada

diasemana = ('segunda-feira','terça-feira','quarta-feira','quinta-feira','sexta-feira','sabado','domingo')

print(diasemana)

# %%

diasemana = tuple('segunda')

print(diasemana)

# %% 

diasemana = tuple(['segunda-feira','terça-feira','quarta-feira','quinta-feira','sexta-feira','sabado','domingo'])

print(diasemana)
type(diasemana)

# %%

diasemana[0] = 'quinta'

# %%

print(diasemana[2])

# %%

type(diasemana)


# %%


# Podemos remover um elemento de uma tupla da seguinte forma

diasemana = tuple(['segunda-feira','terça-feira','quarta-feira','quinta-feira','sexta-feira','sabado','domingo'])

lista_diasemana = list(diasemana)

type(lista_diasemana)

lista_diasemana.pop()

print(lista_diasemana)

diasemana = tuple(lista_diasemana)

print(diasemana)


# %% 

# Fatiamento

diasemana[0:3]