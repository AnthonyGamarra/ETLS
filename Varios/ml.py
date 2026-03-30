# =========================================
# 1. LIBRERÍAS
# =========================================
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
import joblib


# =========================================
# 2. CONEXIÓN A POSTGRESQL
# =========================================
engine = create_engine("postgresql://postgres:4dm1n@10.0.29.117:5433/DW_ESTADISTICA")


# =========================================
# 3. CARGA DE DATOS (CONTROLADA)
# =========================================
query = """
(
SELECT *
FROM dssge.ml_desercion_features
WHERE cod_actividad = '91' AND desercion = 1
LIMIT 200000
)
UNION ALL
(
SELECT *
FROM dssge.ml_desercion_features
WHERE cod_actividad = '91' AND desercion = 0
LIMIT 200000
);
"""

df = pd.read_sql(query, engine)

print("Filas cargadas:", df.shape)


# =========================================
# 4. LIMPIEZA BÁSICA
# =========================================
df = df.dropna()

df['anio_edad'] = pd.to_numeric(df['anio_edad'], errors='coerce')

# opcional: filtrar valores extremos
df = df[(df['anio_edad'] > 0) & (df['anio_edad'] < 110)]


# =========================================
# 5. DEFINIR FEATURES Y TARGET
# =========================================
target = 'desercion'

features = [
    'anio_edad',
    'sexo',
    'cod_servicio',
    'cod_subactividad',
    'dia_semana',
    'mes',
    'es_fin_semana'
]

X = df[features]
y = df[target]


# =========================================
# 6. TRAIN / TEST SPLIT
# =========================================
X_train, X_test, y_train, y_test = train_test_split(
    X, y,
    test_size=0.2,
    random_state=42,
    stratify=y
)


# =========================================
# 7. PREPROCESAMIENTO
# =========================================
categorical = ['sexo', 'cod_subactividad', 'cod_servicio']
numerical = [
    'anio_edad',
    'dia_semana',
    'mes',
    'es_fin_semana'
]

preprocess = ColumnTransformer(
    transformers=[
        ('cat', OneHotEncoder(handle_unknown='ignore'), categorical),
        ('num', 'passthrough', numerical)
    ]
)


# =========================================
# 8. MODELO 1: LOGISTIC REGRESSION
# =========================================
model_lr = Pipeline(steps=[
    ('preprocess', preprocess),
    ('classifier', LogisticRegression(max_iter=1000))
])

model_lr.fit(X_train, y_train)

y_pred_lr = model_lr.predict(X_test)
y_prob_lr = model_lr.predict_proba(X_test)[:, 1]

print("\n===== Logistic Regression =====")
print(classification_report(y_test, y_pred_lr))
print("Confusion Matrix:\n", confusion_matrix(y_test, y_pred_lr))
print("ROC AUC:", roc_auc_score(y_test, y_prob_lr))


# =========================================
# 9. MODELO 2: RANDOM FOREST
# =========================================
model_rf = Pipeline(steps=[
    ('preprocess', preprocess),
    ('classifier', RandomForestClassifier(
        n_estimators=100,
        random_state=42,
        n_jobs=-1
    ))
])

model_rf.fit(X_train, y_train)

y_pred_rf = model_rf.predict(X_test)
y_prob_rf = model_rf.predict_proba(X_test)[:, 1]

print("\n===== Random Forest =====")
print(classification_report(y_test, y_pred_rf))
print("Confusion Matrix:\n", confusion_matrix(y_test, y_pred_rf))
print("ROC AUC:", roc_auc_score(y_test, y_prob_rf))


# =========================================
# 10. AJUSTE DE UMBRAL (NEGOCIO)
# =========================================
threshold = 0.3  # captura más desertores

y_pred_custom = (y_prob_rf > threshold).astype(int)

print("\n===== Random Forest (Threshold 0.3) =====")
print(classification_report(y_test, y_pred_custom))


# =========================================
# 11. GUARDAR RESULTADOS
# =========================================
df_result = X_test.copy()
df_result['real'] = y_test.values
df_result['prob_desercion'] = y_prob_rf
df_result['prediccion'] = y_pred_custom

# opcional: guardar a CSV
df_result.to_csv("predicciones_desercion.csv", index=False)


# =========================================
# 12. EJEMPLO DE PREDICCIÓN NUEVA
# =========================================
nuevo = pd.DataFrame({
    'anio_edad': [25],
    'sexo': ['M'],
    'cod_area': ['01'],
    'cod_servicio': ['AB1'],
    'dia_semana': [1],
    'mes': [3],
    'es_fin_semana': [0],
    'cod_subactividad': ['001']
})

prob = model_rf.predict_proba(nuevo)[:, 1][0]

print("\nProbabilidad de deserción:", prob)

# Guardar el pipeline completo
joblib.dump(model_rf, "modelo_desercion.pkl")

print("Modelo exportado correctamente")