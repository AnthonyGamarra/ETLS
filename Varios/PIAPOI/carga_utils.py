import io
import os
import psycopg2
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# ==============================
# Configuración común PIA / POI
# ==============================
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

RUTA_PRESUPUESTO = r"C:\Users\gcpp.ggi.sge2\Desktop\GGI\presupuesto"
ESQUEMA = "dwsge"


def conectar_pg():
    return psycopg2.connect(
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASS"),
        host=os.getenv("PG_HOST"),
        port=5433,
    )


def leer_excel(archivo, columnas, numericas=(), hoja=0):
    """Lee el Excel como texto (conserva ceros a la izquierda), renombra y limpia."""
    df = pd.read_excel(os.path.join(RUTA_PRESUPUESTO, archivo), sheet_name=hoja, dtype=str)
    df = df[list(columnas.keys())].rename(columns=columnas)
    for c in df.columns:
        df[c] = df[c].str.strip()
    for c in numericas:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def cargar_tabla(df, tabla, ddl):
    """Crea la tabla si no existe, la vacía y la recarga con COPY en una sola transacción.
    Se usa TRUNCATE (no DROP) para no romper la vista materializada que depende de ella."""
    inicio = datetime.now()
    df = df.copy()
    df["fecha_carga"] = inicio
    conn = conectar_pg()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(f"CREATE TABLE IF NOT EXISTS {ESQUEMA}.{tabla} ({ddl}, fecha_carga timestamp);")
            cur.execute(f"TRUNCATE TABLE {ESQUEMA}.{tabla};")
            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            cur.copy_expert(
                f"COPY {ESQUEMA}.{tabla} ({', '.join(df.columns)}) FROM STDIN WITH (FORMAT csv, NULL '')",
                buffer,
            )
        print(f"{ESQUEMA}.{tabla}: {len(df)} filas cargadas en {datetime.now() - inicio}")
    finally:
        conn.close()
