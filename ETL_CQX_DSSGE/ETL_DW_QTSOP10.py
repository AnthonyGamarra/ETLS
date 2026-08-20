import os
import oracledb
import pandas as pd
from io import StringIO
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

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
    limite = (end_date.replace(day=1) + relativedelta(months=1))
    while current < limite:
        start_mes = current
        end_mes = (current.replace(day=1) + relativedelta(months=1))
        yield start_mes, end_mes
        current = end_mes

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
end_date = datetime(2026, 8, 31)

# ==============================
# 6. Ciclo para extraer y copiar mes a mes
# ==============================
for start_mes, end_mes in month_range(start_date, end_date):
    print(f"\n--- Procesando mes: {start_mes.strftime('%Y-%m')} ---")
    anio = start_mes.strftime('%Y')
    mes  = start_mes.strftime('%m')  # <-- Asegura formato 01,02,03...
    start_mes_str = start_mes.strftime('%d-%m-%Y')
    end_mes_str = end_mes.strftime('%d-%m-%Y')
    tabla_destino = f"dssge.sgss_qtsop10_{anio}_{mes}"
    
    query = f"""
    select 
    to_char(solopeprofec, 'yyyymm')                    		 								as PERIODO,
    to_char(solopeprofec, 'yyyy')                             								as ANIO,
    solopeoricenasicod,
    solopecenasicod,
    solopenum,
    solopefec,
    solopeactmednum,
    solopemedtipdocidenpercod,
    solopemedperasisdocidennum,
    solopeinfmed,
    solopesolfec,
    solopeprofec,
    solopeprohor,
    estfiscod,
    estsopcod,
    solopeestregcod,
    solopeusucrecod,
    solopecrefec,
    solopeusumodcod,
    solopemodfec,
    solopecenquicod,
    solopesalopecod,
    solopeesttpo,
    solopearehoscod,
    solopeservhoscod,
    solopeordnum,
    solopeemecod,
    solopesolarehoscod,
    solopesolservhoscod,
    conopecod,
    solopeactmedopenum,
    priopecod,
    riequicod,
    solopediashosprecan,
    solopediashosposcan,
    solopepredetdes,
    solopereqadides,
    motsopcod,
    solopetipanecod,
    soloperesexalabflg,
    soloperiequiflg,
    soloperieneuflg,
    solopeconinfflg,
    solopeordtraflg,
    solopeevalpqxinf,
    solopeevalpqxflg,
    solopeevalpqxfec,
    solopeevalpqxoricenasicod,
    solopeevalpqxcenasicod,
    solopeevalpqxactmednum,
    solopetopemecod,
    solopeatesecnum,
    solopebuspacsecnum,
    solopesolexafec,
    soloperesexalabfec,
    soloperiequifec,
    soloperieneufec,
    solopeevalpqxmedtipdoc,
    solopeevalpqxmeddocnum,
    solopediashospreflg,
    solopediashosposflg,
    solopereqprotflg,
    solopereqprotdes,
    solopetieprotflg,
    solopetlffamnum,
    solopesolexaflg,
    solopesolexaimgflg,
    rieneucod,
    solopesopfec,
    motboqxcod,
    solopeboqxfec,
    soloperesexaimgflg,
    soloperesexaimgfec,
    solopeconinffec,
    solopeordtrafec,
    solopesolexaimgfec,
    solopeotrsopdes,
    solopemedsusptipdoc,
    solopemedsuspdocnum,
    solopemedbajatipdoc,
    solopemedbajadocnum,
    solopeobsseginf,
    solopeopefec,
    solopealtfec,
    solopeotrboqxdes,
    solopehospfec,
    solopeprotfec,
    solopeproconfflg,
    solopeproconffec,
    solopeproconftipdoc,
    solopeproconfdocnum,
    solopehospflg,
    solopeordintoricenasicod,
    solopeordintcenasicod,
    solopeordintnum,
    solopehorqxdifflg,
    solopeconinfaneflg,
    solopeconinfanefec,
    solopeconinfsanflg,
    solopeconinfsanfec,
    solopeconinfdesflg,
    solopeconinfdesfec,
    solopetipeveope
       from sgss.qtsop10
    where
            solopeprofec >= TO_DATE('{start_mes_str}', 'DD-MM-YYYY')
            and solopeprofec < TO_DATE('{end_mes_str}', 'DD-MM-YYYY')
    """

    print(f"Ejecutando query para mes {start_mes.strftime('%Y-%m')} en Oracle...")
    df = pd.read_sql(query, conn_oracle)
    print(f"Datos extraídos: {len(df)} filas.")

    if df.empty:
        print("No hay datos para este mes.")
        continue

    df.columns = df.columns.str.lower()

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




