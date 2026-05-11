import os
import oracledb
import pandas as pd
from io import StringIO
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta

# ==============================
# 1. Cargar variables desde .env
# ==============================
load_dotenv()

# Oracle
oracle_user = os.getenv("ORACLE_USER")
oracle_pass = os.getenv("ORACLE_PASS")
oracle_host = os.getenv("ORACLE_HOST")
oracle_port = os.getenv("ORACLE_PORT")
oracle_service = os.getenv("ORACLE_SERVICE")

# PostgreSQL
pg_user = os.getenv("PG_USER")
pg_pass = os.getenv("PG_PASS")
pg_host = os.getenv("PG_HOST")
pg_port = os.getenv("PG_PORT", "5433")
pg_db   = os.getenv("PG_DB")

# ==============================
# 2. Función para obtener rango mensual
# ==============================
def month_range(start_date, end_date):
    current = start_date.replace(day=1)
    while current <= end_date:
        # primer día del siguiente mes
        next_month = (current.replace(day=1) + timedelta(days=32)).replace(day=1)
        yield current, next_month
        current = next_month

# ==============================
# 3. Conexión Oracle
# ==============================
print(f"Conectando a Oracle en {oracle_host}:{oracle_port}/{oracle_service}...")
dsn = f"{oracle_host}:{oracle_port}/{oracle_service}"
conn_oracle = oracledb.connect(user=oracle_user, password=oracle_pass, dsn=dsn)
print("Conexión a Oracle establecida.")

# ==============================
# 4. Conexión PostgreSQL
# ==============================
print(f"Conectando a PostgreSQL en {pg_host}:{pg_port}, base de datos: {pg_db}...")
conn_pg = psycopg2.connect(
    host=pg_host,
    database=pg_db,
    user=pg_user,
    password=pg_pass,
    port=pg_port
)
conn_pg.autocommit = True
cursor_pg = conn_pg.cursor()
print("Conexión a PostgreSQL establecida.")

# ==============================
# 5. Parámetros de fechas
# ==============================
start_date = datetime(2026, 5, 1)
end_date = datetime(2026, 5, 31)


start_time = datetime.now()
print(f"\n🕒 Inicio del ETL: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"\n--- Iniciando extracción mes a mes entre {start_date.strftime('%Y-%m-%d')} y {end_date.strftime('%Y-%m-%d')} ---")

# ==============================
# 6. Procesar mes a mes sin paginación
# ==============================
for start_mes, start_next_mes in month_range(start_date, end_date):
    anio = start_mes.strftime('%Y')
    mes = start_mes.strftime('%m')
    print(f"\nProcesando mes: {start_mes.strftime('%Y-%m')}")

    query = f"""
            SELECT 
                a.ATENOMORICENASICOD, 
                a.ATENOMCENASICOD, 
                a.ATENOMACTMEDNUM, 
                a.ATENMDCONDDIAGCOD, 
                a.ATENMDDIAGCOD, 
                a.ATENMDDIAGORD, 
                a.ATENMDTIPODIAGCOD, 
                a.ATENMDCASODIAGCOD, 
                a.ATENMDALTAFLG, 
                a.ATENMDPEAS,
                TO_CHAR(TRUNC(c.atenomfec), 'yyyymm') AS periodo,
                TO_CHAR(TRUNC(c.atenomfec), 'yyyy') AS anio
            FROM sgss.ctdan10 a
            LEFT OUTER JOIN sgss.ctanm10 c 
                ON c.atenomoricenasicod = a.atenomoricenasicod
            AND c.atenomcenasicod    = a.atenomcenasicod
            AND c.atenomactmednum          = a.atenomactmednum
            WHERE c.atenomestregcod = '1'
        AND atenomfec >= TO_DATE('{start_mes.strftime('%d-%m-%Y')}', 'DD-MM-YYYY')
        AND atenomfec < TO_DATE('{start_next_mes.strftime('%d-%m-%Y')}', 'DD-MM-YYYY')
        ORDER BY atenomfec
    """

    print(f"Ejecutando query para mes {start_mes.strftime('%Y-%m')} en Oracle...")
    df = pd.read_sql(query, conn_oracle)
    print(f"Datos extraídos: {len(df)} filas.")

    if df.empty:
        print(f"No hay datos para el mes {start_mes.strftime('%Y-%m')}.")
        continue

    df.columns = df.columns.str.lower()


    # Truncar la tabla particionada destino en PostgreSQL antes de la carga
    tabla_particion = f"dssge.sgss_ctdan10_anio_v2_{anio}_{mes}"
    try:
        print(f"Truncando tabla particionada destino: {tabla_particion}...")
        cursor_pg.execute(f"TRUNCATE TABLE {tabla_particion};")
        conn_pg.commit()
        print(f"Tabla {tabla_particion} truncada correctamente.")
    except Exception as e:
        print(f"Error al truncar la tabla {tabla_particion}: {e}")
        continue

    # Guardamos el DataFrame en un buffer CSV en memoria
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)

    print(f"Cargando datos a PostgreSQL para mes {start_mes.strftime('%Y-%m')}...")
    try:
        cursor_pg.copy_expert(
            sql=f"COPY dssge.sgss_ctdan10_anio_v2 ({', '.join(df.columns)}) FROM STDIN WITH CSV",
            file=csv_buffer
        )
        print(f"Mes {start_mes.strftime('%Y-%m')} cargado correctamente.")
    except Exception as e:
        print(f"Error al cargar mes {start_mes.strftime('%Y-%m')}: {e}")

# ==============================
# 7. Cerramos conexiones
# ==============================
print("\nCerrando conexiones a bases de datos...")
cursor_pg.close()
conn_pg.close()
conn_oracle.close()
print("Conexiones cerradas. Proceso finalizado.")