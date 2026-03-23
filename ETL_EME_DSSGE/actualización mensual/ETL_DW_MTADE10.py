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
    current = start_date
    while current <= end_date:
        next_month = (current.replace(day=1) + timedelta(days=32)).replace(day=1)
        yield current, min(next_month - timedelta(days=1), end_date)
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
start_date = datetime(2026, 3, 1)
end_date = datetime(2026, 3, 31)

# ==============================
# 6. Ciclo para extraer y copiar mes a mes
# ==============================
for start_mes, end_mes in month_range(start_date, end_date):
    print(f"\n--- Procesando mes: {start_mes.strftime('%Y-%m')} ---")
    anio = start_mes.strftime('%Y')
    mes  = start_mes.strftime('%m')  # <-- Asegura formato 01,02,03...
    tabla_destino = f"dssge.sgss_mtade10_{anio}_{mes}"
    

    query = f"""
            SELECT 
                a.ADMEMEORICENASICOD,
                a.ADMEMECENASICOD,
                a.ADMEMEACTMEDNUM,
                a.ADMEMEAREHOSCOD,
                a.ADMEMEEMECOD,
                to_char(a.ADMEMEALTADMFEC, 'yyyy') as anio,
                to_char(a.ADMEMEALTADMFEC, 'yyyymm') as periodo,                
                to_char(a.ADMEMEADMFEC, 'DD-MM-YYYY') AS ADMEMEADMFEC,
                to_char(a.ADMEMEADMHOR, 'HH24:MI:SS') AS ADMEMEADMHOR,
                a.TIPACCCOD,
                a.ACOPACCOD,
                a.ADMEMEEGRFLG,
                to_char(a.ADMEMEALTMEDFEC, 'DD-MM-YYYY') AS ADMEMEALTMEDFEC,
                to_char(a.ADMEMEALTMEDHOR, 'HH24:MI:SS') AS ADMEMEALTMEDHOR,
                a.ADMEMEESTREGCOD,
                a.ADMEMETOPEMECOD,
                to_char(a.ADMEMEALTADMFEC, 'DD-MM-YYYY') AS ADMEMEALTADMFEC,
                to_char(a.ADMEMEALTADMHOR, 'HH24:MI:SS') AS ADMEMEALTADMHOR,
                a.ADMEMEATETRIORICENASICOD,
                a.ADMEMEATETRICENASICOD,
                a.ADMEMEATETRIAREHOSCOD,
                a.ADMEMEATETRIEMECOD,
                a.ADMEMEATETRIANO,
                a.ADMEMEATETRINUM,
                a.ADMEMEMOTEGRCOD,
                a.ADMEMEULTPRIATECOD,
                a.ADMEMEPACSECNUM,
                a.ADMEMESOSCOVID,
                b.TIPCONCOD,
                b.ACTMEDFAC,
                b.ACTMEDFECATEN,
                b.ACTMEDTIPOPARDIACOD,
                b.ACTMEDTIPOPARPROCOD,
                b.ACTMEDATE,
                b.ACTMEDHOS,
                b.ACTMEDCITT,
                b.ACTMEDOPE,
                b.ACTMEDESTREGCOD,
                b.ACTMEDTIPATECOD,
                b.ACTMEDAFESCTRFLG,   
                b.ORICENASICOD,
                b.CENASICOD,
                b.ACTMEDNUM,
                b.ACTMEDPACSECNUM,
                b.ACTMEDAREHOSCOD,
                b.ACTMEDSERVHOSCOD,
                b.ACTMEDTIPSEGCOD,
                b.ACTMEDPLANTIPSEGCOD,          
                b.ACTMEDTIPOPACICOD,
                b.TIPOPARECOD
            FROM SGSS.MTADE10 a
            LEFT OUTER JOIN SGSS.CMAME10 b ON a.ADMEMEORICENASICOD=b.ORICENASICOD
                            AND a.ADMEMECENASICOD=b.CENASICOD
                            AND a.ADMEMEACTMEDNUM=b.ACTMEDNUM
            WHERE ADMEMEALTADMFEC >= TO_DATE('{start_mes.strftime('%d-%m-%Y')}', 'DD-MM-YYYY')
            and ADMEMEALTADMFEC < TO_DATE('{(end_mes + timedelta(days=1)).strftime('%d-%m-%Y')}', 'DD-MM-YYYY')

    """

    print(f"Ejecutando query para mes {start_mes.strftime('%Y-%m')} en Oracle...")
    df = pd.read_sql(query, conn_oracle)
    print(f"Datos extraídos: {len(df)} filas.")

    if df.empty:
        print("No hay datos para este mes.")
        continue

    # ==============================
    # TRUNCAR PARTICIÓN DESTINO
    # ==============================
    print(f"Truncando partición destino: {tabla_destino}...")
    try:
        cursor_pg.execute(f"TRUNCATE TABLE {tabla_destino};")
        print(f"Tabla {tabla_destino} truncada correctamente.")
    except Exception as e:
        print(f"⚠️ Error al truncar {tabla_destino}: {e}")
        continue  # Saltar este mes si no existe la partición

    df.columns = df.columns.str.lower()

    # Guardamos el DataFrame en un buffer CSV en memoria
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)

    # Usamos COPY para cargar datos a PostgreSQL
    print(f"Cargando datos a PostgreSQL para mes {start_mes.strftime('%Y-%m')}...")
    try:
        cursor_pg.copy_expert(
            sql=f"COPY {tabla_destino} ({', '.join(df.columns)}) FROM STDIN WITH CSV",
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