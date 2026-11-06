
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
load_dotenv()

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
# También se puede ejecutar: python ETL_DW_DATA_SES.py 201901 202609
MODO = "HISTORICO"
PERIODO_INI = 201901
PERIODO_FIN = 202609

# Procedimientos almacenados a unir (agregar aquí otros SP con la misma estructura)
PROCEDIMIENTOS = [
    "dbo.USP_FONAFE_GCOP_V3"
]

tabla_destino = "dwsge.data_ses"

# Columnas destino en el mismo orden que devuelve el SP
columnas_destino = [
    "familias", "codpre", "unidad_medida", "codred_ses", "red_ses",
    "codcentro_ses", "desccentro_ses", "cod_nivel", "mes", "año",
    "cod_grupo", "desc_grupo", "cod_acti_ses", "desc_act_ses",
    "cod_actividad", "descripcion_actividad", "asegurado", "no_asegurados",
    "nivel_atencion",
]

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
# 5. Conexión PostgreSQL
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
print("Conexión a PostgreSQL establecida.")

# ==============================
# 6. Función para ejecutar un SP y devolver DataFrame
# ==============================
def ejecutar_sp(procedimiento, periodo):
    cursor_sql.execute(
        f"SET NOCOUNT ON; EXEC {procedimiento} @PERIODOI = ?, @PERIODOF = ?;",
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
            f"{procedimiento} devolvió {df.shape[1]} columnas, "
            f"se esperaban {len(columnas_destino)}."
        )
    df.columns = columnas_destino
    return df

# ==============================
# 7. Ciclo mes a mes: extraer, borrar mes en destino y cargar
# ==============================
columnas_sql = ", ".join(f'"{c}"' for c in columnas_destino)

for periodo in month_range(periodo_ini, periodo_fin):
    print(f"\n--- Procesando periodo: {periodo} ---")

    try:
        df = pd.concat(
            [ejecutar_sp(sp, periodo) for sp in PROCEDIMIENTOS],
            ignore_index=True
        )
    except Exception as e:
        print(f"⚠️ Error al ejecutar SP para {periodo}: {e}")
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

# ==============================
# 8. Refrescar vista materializada
# ==============================
print("\nRefrescando vista materializada dwsge.mv_data_ses...")
try:
    cursor_pg.execute("REFRESH MATERIALIZED VIEW dwsge.mv_data_ses;")
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
print("Conexiones cerradas. Proceso finalizado.")
