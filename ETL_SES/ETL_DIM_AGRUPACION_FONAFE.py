"""
Carga agrupacion_fonafe.xlsx (primera hoja) -> dwsge.agrupacion_fonafe

Mapea cada actividad FONAFE (catálogo de dbo.USP_FONAFE_GCOP_V3, 36 actividades)
a su grupo FONAFE. Llave: cod_actividad.
Reemplaza todo el contenido de la tabla y refresca dwsge.mv_data_ses_fonafe.

Uso: python ETL_DIM_AGRUPACION_FONAFE.py
"""
import os
import pandas as pd
import psycopg2
from io import StringIO
from dotenv import load_dotenv

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(SCRIPT_DIR, ".env"))

pg_user = os.getenv("PG_USER")
pg_pass = os.getenv("PG_PASS")
pg_host = os.getenv("PG_HOST")
pg_port = os.getenv("PG_PORT", "5433")
pg_db   = os.getenv("PG_DB")

ARCHIVO = os.path.join(SCRIPT_DIR, "agrupacion_fonafe.xlsx")
TABLA = "dwsge.agrupacion_fonafe"
VISTA = "dwsge.mv_data_ses_fonafe"
COLUMNAS = ["cod_actividad", "descripcion_actividad", "grupo_fonafe"]

# ==============================
# 1. Leer y validar Excel
# ==============================
df = pd.read_excel(ARCHIVO, sheet_name=0)
df.columns = df.columns.str.strip().str.lower()
faltantes = [c for c in COLUMNAS if c not in df.columns]
if faltantes:
    raise SystemExit(f"Faltan columnas en el Excel: {faltantes}")
df = df[COLUMNAS].dropna(subset=["cod_actividad"])
df["cod_actividad"] = df["cod_actividad"].astype(int)
for col in ("descripcion_actividad", "grupo_fonafe"):
    df[col] = df[col].astype(str).str.strip()

duplicados = df[df.duplicated("cod_actividad", keep=False)]
if not duplicados.empty:
    raise SystemExit(f"Hay cod_actividad duplicados en el Excel:\n{duplicados}")
print(f"Excel leído: {len(df)} actividades, {df['grupo_fonafe'].nunique()} grupos FONAFE.")

# ==============================
# 2. Cargar a PostgreSQL
# ==============================
conn = psycopg2.connect(host=pg_host, port=pg_port, dbname=pg_db, user=pg_user, password=pg_pass)
cursor = conn.cursor()
try:
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLA} (
            cod_actividad          integer PRIMARY KEY,
            descripcion_actividad  varchar(200),
            grupo_fonafe           varchar(100)
        );
    """)
    cursor.execute(f"TRUNCATE TABLE {TABLA};")
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)
    cursor.copy_expert(f"COPY {TABLA} ({', '.join(COLUMNAS)}) FROM STDIN WITH CSV", csv_buffer)
    print(f"{len(df)} filas cargadas en {TABLA}.")

    cursor.execute(f"SELECT to_regclass('{VISTA}');")
    if cursor.fetchone()[0]:
        cursor.execute(f"REFRESH MATERIALIZED VIEW {VISTA};")
        print(f"Vista {VISTA} refrescada.")
    conn.commit()
except Exception:
    conn.rollback()
    raise
finally:
    cursor.close()
    conn.close()
