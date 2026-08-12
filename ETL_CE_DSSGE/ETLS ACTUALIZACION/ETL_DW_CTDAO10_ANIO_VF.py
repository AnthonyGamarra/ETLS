import os
import oracledb
import polars as pl
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
start_date = datetime(2026, 8, 1)
end_date = datetime(2026, 8, 30)

start_time = datetime.now()
print(f"\n🕒 Inicio del ETL: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"\n--- Iniciando extracción mes a mes entre {start_date.strftime('%Y-%m-%d')} y {end_date.strftime('%Y-%m-%d')} ---")

# ==============================
# 6. Procesar mes a mes sin paginación
# ==============================
for start_mes, start_next_mes in month_range(start_date, end_date):
    mes_start_time = datetime.now()
    anio = start_mes.strftime('%Y')
    mes = start_mes.strftime('%m')
    print(f"\nProcesando mes: {start_mes.strftime('%Y-%m')}")
    print(f"🕒 Inicio del mes: {mes_start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    query = f"""
            SELECT 
                a.ATENODOORICENASICOD, 
                a.ATENODOCENASICOD, 
                a.ATENODONUM, 
                a.CONDDIAGCOD, 
                a.DIAGCOD, 
                a.ATENODODIAGORD, 
                a.TIPODIAGCOD, 
                a.CASODIAGCOD, 
                a.DIAGATENODOALTAFLAG, 
                a.DIAGATENODOPEAS,
                TO_CHAR(TRUNC(c.ATENODOATENFEC), 'yyyymm') AS periodo,
                TO_CHAR(TRUNC(c.ATENODOATENFEC), 'yyyy') AS anio
            FROM sgss.ctdao10 a
            LEFT OUTER JOIN sgss.ctaod10 c 
                ON c.atenodooricenasicod = a.ATENODOORICENASICOD
            AND c.atenodocenasicod    = a.ATENODOCENASICOD
            AND c.atenodonum          = a.ATENODONUM
            WHERE c.ATENODOESTREGCOD = '1'
        AND ATENODOATENFEC >= TO_DATE('{start_mes.strftime('%d-%m-%Y')}', 'DD-MM-YYYY')
        AND ATENODOATENFEC < TO_DATE('{start_next_mes.strftime('%d-%m-%Y')}', 'DD-MM-YYYY')
        ORDER BY ATENODOATENFEC
    """

    print(f"Ejecutando query para mes {start_mes.strftime('%Y-%m')} en Oracle...")
    extraccion_start = datetime.now()
    df = pl.read_database(query=query, connection=conn_oracle, infer_schema_length=None)
    extraccion_time = datetime.now() - extraccion_start
    print(f"Datos extraídos: {len(df)} filas. (tiempo de extracción: {extraccion_time})")

    if df.is_empty():
        print(f"No hay datos para el mes {start_mes.strftime('%Y-%m')}.")
        continue

    df.columns = [c.lower() for c in df.columns]


    # Truncar la tabla particionada destino en PostgreSQL antes de la carga
    tabla_particion = f"dssge.sgss_ctdao10_anio_v2_{anio}_{mes}"
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
    df.write_csv(
        csv_buffer,
        include_header=False,
        datetime_format="%Y-%m-%d %H:%M:%S",
        date_format="%Y-%m-%d",
    )
    csv_buffer.seek(0)

    print(f"Cargando datos a PostgreSQL para mes {start_mes.strftime('%Y-%m')}...")
    carga_start = datetime.now()
    try:
        cursor_pg.copy_expert(
            sql=f"COPY dssge.sgss_ctdao10_anio_v2 ({', '.join(df.columns)}) FROM STDIN WITH CSV",
            file=csv_buffer
        )
        carga_time = datetime.now() - carga_start
        print(f"Mes {start_mes.strftime('%Y-%m')} cargado correctamente. (tiempo de carga: {carga_time})")
    except Exception as e:
        print(f"Error al cargar mes {start_mes.strftime('%Y-%m')}: {e}")

    mes_end_time = datetime.now()
    print(f"⏱️ Mes {start_mes.strftime('%Y-%m')} procesado en {mes_end_time - mes_start_time}")

# ==============================
# 7. Cerramos conexiones
# ==============================
print("\nCerrando conexiones a bases de datos...")
cursor_pg.close()
conn_pg.close()
conn_oracle.close()
print("Conexiones cerradas. Proceso finalizado.")

end_time = datetime.now()
print(f"\n🕒 Fin del ETL: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"⏱️ Tiempo total de procesamiento: {end_time - start_time}")