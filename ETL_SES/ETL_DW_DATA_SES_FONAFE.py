"""
ETL dbo.USP_FONAFE_GCOP_V3 (SQL Server SES) -> dwsge.data_ses_fonafe (PostgreSQL)

Procesa mes a mes: ejecuta el SP para el periodo, borra ese año/mes en destino
y carga la data nueva. Cada mes va en su propia transacción.

Uso:
  python ETL_DW_DATA_SES_FONAFE.py                 -> según MODO (MENSUAL: mes anterior)
  python ETL_DW_DATA_SES_FONAFE.py 201901 202609   -> rango de periodos
"""
import os
import sys
import pyodbc
import pandas as pd
from io import StringIO
import psycopg2
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta

# ==============================
# 1. Cargar variables desde .env
# ==============================
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# SQL Server (SES)
sql_host = os.getenv("SQL_HOST")
sql_port = os.getenv("SQL_PORT", "1433")
sql_user = os.getenv("SQL_USER")
sql_pass = os.getenv("SQL_PASS")
sql_db   = os.getenv("SQL_DB")  # opcional: si no se define, usa la BD por defecto del usuario
sql_driver = os.getenv("SQL_DRIVER", "SQL Server")

# PostgreSQL
pg_user = os.getenv("PG_USER")
pg_pass = os.getenv("PG_PASS")
pg_host = os.getenv("PG_HOST")
pg_port = os.getenv("PG_PORT", "5433")
pg_db   = os.getenv("PG_DB")

# ==============================
# 2. Parámetros
# ==============================
# MODO = "MENSUAL"   -> procesa solo el mes anterior al actual (actualización mensual)
# MODO = "HISTORICO" -> procesa desde PERIODO_INI hasta PERIODO_FIN (carga inicial)
MODO = "MENSUAL"
PERIODO_INI = 201901
PERIODO_FIN = 202609

PROCEDIMIENTO = "dbo.USP_FONAFE_GCOP_V3"
tabla_destino = "dwsge.data_ses_fonafe"

# Columnas destino en el mismo orden que devuelve el SP
columnas_destino = [
    "codred_ses", "red_ses", "codcentro_ses", "desccentro_ses", "nivel",
    "mes", "año", "cod_actividad", "descripcion_actividad",
    "asegurados", "no_asegurados", "nivel_atencion",
]

DDL = f"""
CREATE TABLE IF NOT EXISTS {tabla_destino} (
    codred_ses             integer,
    red_ses                varchar(200),
    codcentro_ses          integer,
    desccentro_ses         varchar(200),
    nivel                  varchar(50),
    mes                    varchar(20),
    "año"                  varchar(4),
    cod_actividad          integer,
    descripcion_actividad  varchar(200),
    asegurados             integer,
    no_asegurados          integer,
    nivel_atencion         varchar(50)
);
CREATE INDEX IF NOT EXISTS ix_data_ses_fonafe_periodo ON {tabla_destino} ("año", mes);
"""

if len(sys.argv) == 3:
    periodo_ini, periodo_fin = int(sys.argv[1]), int(sys.argv[2])
elif MODO == "HISTORICO":
    periodo_ini, periodo_fin = PERIODO_INI, PERIODO_FIN
else:
    mes_anterior = datetime.today().replace(day=1) - relativedelta(months=1)
    periodo_ini = periodo_fin = int(mes_anterior.strftime("%Y%m"))

# ==============================
# 3. Función para obtener rango mensual (yyyymm)
# ==============================
def month_range(periodo_ini, periodo_fin):
    current = datetime.strptime(str(periodo_ini), "%Y%m")
    limite = datetime.strptime(str(periodo_fin), "%Y%m")
    while current <= limite:
        yield int(current.strftime("%Y%m"))
        current += relativedelta(months=1)

# ==============================
# 4. Conexión SQL Server
# ==============================
print(f"Conectando a SQL Server en {sql_host}:{sql_port}...")
conn_str = (
    f"DRIVER={{{sql_driver}}};SERVER={sql_host},{sql_port};"
    f"UID={sql_user};PWD={sql_pass};"
)
if sql_db:
    conn_str += f"DATABASE={sql_db};"
conn_sql = pyodbc.connect(conn_str)
cursor_sql = conn_sql.cursor()
print("Conexión a SQL Server establecida.")

# ==============================
# 5. Conexión PostgreSQL y creación de tabla
# ==============================
print(f"Conectando a PostgreSQL en {pg_host}:{pg_port}, base de datos: {pg_db}...")
conn_pg = psycopg2.connect(
    host=pg_host,
    database=pg_db,
    user=pg_user,
    password=pg_pass,
    port=pg_port
)
cursor_pg = conn_pg.cursor()
cursor_pg.execute(DDL)
conn_pg.commit()
print(f"Conexión a PostgreSQL establecida. Tabla {tabla_destino} lista.")

# ==============================
# 6. Función para ejecutar el SP y devolver DataFrame
# ==============================
def ejecutar_sp(periodo):
    cursor_sql.execute(
        f"SET NOCOUNT ON; EXEC {PROCEDIMIENTO} @PERIODOI = ?, @PERIODOF = ?;",
        periodo, periodo
    )
    # Saltar resultados intermedios sin columnas
    while cursor_sql.description is None:
        if not cursor_sql.nextset():
            return pd.DataFrame(columns=columnas_destino)
    filas = cursor_sql.fetchall()
    while cursor_sql.nextset():
        pass
    df = pd.DataFrame.from_records([tuple(f) for f in filas])
    if df.empty:
        return pd.DataFrame(columns=columnas_destino)
    if df.shape[1] != len(columnas_destino):
        raise ValueError(
            f"{PROCEDIMIENTO} devolvió {df.shape[1]} columnas, "
            f"se esperaban {len(columnas_destino)}."
        )
    df.columns = columnas_destino
    return df

# ==============================
# 7. Ciclo mes a mes: extraer, borrar mes en destino y cargar
# ==============================
columnas_sql = ", ".join(f'"{c}"' for c in columnas_destino)
errores = []

for periodo in month_range(periodo_ini, periodo_fin):
    print(f"\n--- Procesando periodo: {periodo} ---")

    try:
        df = ejecutar_sp(periodo)
    except Exception as e:
        print(f"⚠️ Error al ejecutar SP para {periodo}: {e}")
        errores.append(periodo)
        continue

    print(f"Datos extraídos: {len(df)} filas.")
    if df.empty:
        print("No hay datos para este periodo.")
        continue

    # Periodos (año, mes) que trae el SP: se borran antes de insertar para evitar duplicados
    periodos_df = df[["año", "mes"]].astype(str).drop_duplicates().values.tolist()

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)

    try:
        for anio, mes in periodos_df:
            cursor_pg.execute(
                f'DELETE FROM {tabla_destino} WHERE "año" = %s AND mes = %s;',
                (anio, mes)
            )
            print(f"Eliminadas {cursor_pg.rowcount} filas previas de año={anio}, mes={mes}.")

        cursor_pg.copy_expert(
            sql=f"COPY {tabla_destino} ({columnas_sql}) FROM STDIN WITH CSV",
            file=csv_buffer
        )
        conn_pg.commit()
        print(f"Periodo {periodo} cargado correctamente.")
    except Exception as e:
        conn_pg.rollback()
        print(f"Error al cargar periodo {periodo}: {e}")
        errores.append(periodo)

# ==============================
# 8. Refrescar vista materializada (data FONAFE + grupo_fonafe)
# ==============================
print("\nRefrescando vista materializada dwsge.mv_data_ses_fonafe...")
try:
    cursor_pg.execute("REFRESH MATERIALIZED VIEW dwsge.mv_data_ses_fonafe;")
    conn_pg.commit()
    print("Vista refrescada.")
except Exception as e:
    conn_pg.rollback()
    print(f"Error al refrescar la vista: {e}")

# ==============================
# 9. Cerramos conexiones
# ==============================
print("\nCerrando conexiones a bases de datos...")
cursor_pg.close()
conn_pg.close()
cursor_sql.close()
conn_sql.close()
if errores:
    print(f"Periodos con error (reprocesar): {errores}")
print("Conexiones cerradas. Proceso finalizado.")
