import joblib
import pandas as pd

# Cargar modelo
modelo = joblib.load("modelo_desercion.pkl")

# Nuevo dato
nuevo = pd.DataFrame({
    'anio_edad':[25],
    'sexo':['M'],
    'cod_area':['01'],
    'cod_servicio':['AM13'],
    'dia_semana':[6],
    'mes':[7],
    'es_fin_semana':[1],
    'cod_subactividad': ['001']
})

# Predicción
prob = modelo.predict_proba(nuevo)[:,1][0]

print("Probabilidad de deserción:", prob)