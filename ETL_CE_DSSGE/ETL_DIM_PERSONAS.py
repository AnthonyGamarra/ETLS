import os
import csv
import oracledb
import psycopg2
from io import StringIO
from dotenv import load_dotenv
from datetime import datetime

# ==============================
# 1. Cargar variables desde .env
# ==============================
load_dotenv()

# ORACLE ORIGEN
oracle_user = os.getenv("ORACLE_USER")
oracle_pass = os.getenv("ORACLE_PASS")
oracle_host = os.getenv("ORACLE_HOST")
oracle_port = os.getenv("ORACLE_PORT")
oracle_service = os.getenv("ORACLE_SERVICE")

# POSTGRES DESTINO
pg_user_dst = os.getenv("PG_USER")
pg_pass_dst = os.getenv("PG_PASS")
pg_host_dst = os.getenv("PG_HOST")
pg_db_dst   = os.getenv("PG_DB")

print(f"🔵 Base de datos ORIGEN (ORACLE):  {oracle_service}")
print(f"🟢 Base de datos DESTINO (POSTGRES): {pg_db_dst}")

# ==============================
# 2. Conexión ORACLE
# ==============================
oracle_dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={oracle_host})(PORT={oracle_port}))(CONNECT_DATA=(SERVICE_NAME={oracle_service})))"

conn_src = oracledb.connect(
    user=oracle_user,
    password=oracle_pass,
    dsn=oracle_dsn
)

cursor_src = conn_src.cursor()

# ==============================
# 3. Conexión POSTGRES
# ==============================
conn_dst = psycopg2.connect(
    dbname=pg_db_dst,
    user=pg_user_dst,
    password=pg_pass_dst,
    host=pg_host_dst,
    port=5433
)

# ==============================
# CONFIG
# ==============================
chunksize = 500000
data_found = False

start_time = datetime.now()
print(f"\n🕒 Inicio del ETL: {start_time:%Y-%m-%d %H:%M:%S}")

try:
    query = """
        SELECT 
            REPLACE(p.PERSECNUM, CHR(0), '') AS PERSECNUM,
            REPLACE(p.PERTIPDOCIDENCOD, CHR(0), '') AS PERTIPDOCIDENCOD,
            REPLACE(p.PERDOCIDENNUM, CHR(0), '') AS PERDOCIDENNUM,
            REPLACE(p.PERAPEPATDES, CHR(0), '') AS PERAPEPATDES,
            REPLACE(p.PERAPEMATDES, CHR(0), '') AS PERAPEMATDES,
            REPLACE(p.PERPRINOMDES, CHR(0), '') AS PERPRINOMDES,
            REPLACE(p.PERSEGNOMDES, CHR(0), '') AS PERSEGNOMDES,
            TO_CHAR(p.PERNACFEC,'dd/mm/yyyy') AS PERNACFEC,
            REPLACE(p.PERSEXOCOD, CHR(0), '') AS PERSEXOCOD,
            REPLACE(p.PERESTCIVCOD, CHR(0), '') AS PERESTCIVCOD,
            REPLACE(p.PERTIPSEGCOD, CHR(0), '') AS PERTIPSEGCOD,
            REPLACE(p.PERAUTCOD, CHR(0), '') AS PERAUTCOD,
            REPLACE(p.PERUBIGEONACNOM, CHR(0), '') AS PERUBIGEONACNOM,
            REPLACE(p.PEREMPASECOD, CHR(0), '') AS PEREMPASECOD,
            REPLACE(p.PERORICENASIADSCOD, CHR(0), '') AS PERORICENASIADSCOD,
            REPLACE(p.PERCENASIADSCOD, CHR(0), '') AS PERCENASIADSCOD,
            TO_CHAR(p.PERINSFEC,'dd/mm/yyyy') AS PERINSFEC,
            TO_CHAR(p.PERVIGFEC,'dd/mm/yyyy') AS PERVIGFEC,
            TO_CHAR(p.PERFALFEC,'dd/mm/yyyy') AS PERFALFEC,
            REPLACE(p.PERCALDOMNOM, CHR(0), '') AS PERCALDOMNOM,
            REPLACE(p.PERNMKDOMNUM, CHR(0), '') AS PERNMKDOMNUM,
            REPLACE(p.PERINLDOMNUM, CHR(0), '') AS PERINLDOMNUM,
            REPLACE(p.PERURBDOMNOM, CHR(0), '') AS PERURBDOMNOM,
            REPLACE(p.PERUBIGEODOMNOM, CHR(0), '') AS PERUBIGEODOMNOM,
            REPLACE(p.PERSECTITNUM, CHR(0), '') AS PERSECTITNUM,
            REPLACE(p.PERCITTFLG, CHR(0), '') AS PERCITTFLG,
            REPLACE(p.PERAFESCTRFLG, CHR(0), '') AS PERAFESCTRFLG,
            REPLACE(p.PERAFEESSVIDFLG, CHR(0), '') AS PERAFEESSVIDFLG,
            REPLACE(p.PERLATFLG, CHR(0), '') AS PERLATFLG,
            REPLACE(p.PERFACFLG, CHR(0), '') AS PERFACFLG,
            REPLACE(p.PERCERTMEDNUM, CHR(0), '') AS PERCERTMEDNUM,
            REPLACE(p.PERPLANSALUCOD, CHR(0), '') AS PERPLANSALUCOD,
            REPLACE(p.PERTIPOPARECOD, CHR(0), '') AS PERTIPOPARECOD,
            REPLACE(p.PERESTREGCOD, CHR(0), '') AS PERESTREGCOD,
            REPLACE(p.PERUSUCRECOD, CHR(0), '') AS PERUSUCRECOD,
            TO_CHAR(p.PERCREFEC,'dd/mm/yyyy') AS PERCREFEC,
            REPLACE(p.PERUSUMODCOD, CHR(0), '') AS PERUSUMODCOD,
            TO_CHAR(p.PERMODFEC,'dd/mm/yyyy') AS PERMODFEC,
            REPLACE(p.PERACRCOMTIPCOD, CHR(0), '') AS PERACRCOMTIPCOD,
            REPLACE(p.PERACRCOMNUM, CHR(0), '') AS PERACRCOMNUM,
            REPLACE(p.PERACRCOMMOTCOD, CHR(0), '') AS PERACRCOMMOTCOD,
            REPLACE(p.PERINTPREAUTCOD, CHR(0), '') AS PERINTPREAUTCOD,
            REPLACE(p.PERUBIGEONAC, CHR(0), '') AS PERUBIGEONAC,
            REPLACE(p.PERRUCEMPNUM, CHR(0), '') AS PERRUCEMPNUM,
            REPLACE(p.PERUBIGEODOM, CHR(0), '') AS PERUBIGEODOM,
            REPLACE(p.PERAUTTITCOD, CHR(0), '') AS PERAUTTITCOD,
            TO_CHAR(p.PERAPORPERIO,'dd/mm/yyyy') AS PERAPORPERIO,
            REPLACE(p.PERRAZANEGRAFLG, CHR(0), '') AS PERRAZANEGRAFLG,
            REPLACE(p.PERINDATE, CHR(0), '') AS PERINDATE,
            REPLACE(p.PERBLOHC, CHR(0), '') AS PERBLOHC
        FROM sgss.cmper10 p
    """

    cursor_src.execute(query)

    rows_processed = 0
    colnames = [col[0].lower() for col in cursor_src.description]
    copy_sql = f"""
        COPY dssge.dwe_personas_essi ({', '.join(colnames)})
        FROM STDIN WITH CSV
    """

    while True:
        rows = cursor_src.fetchmany(chunksize)
        if not rows:
            break

        data_found = True

        print(f"📥 Batch leído: {len(rows)} filas")

        buffer = StringIO()
        writer = csv.writer(buffer, lineterminator='\n')
        writer.writerows(rows)
        buffer.seek(0)

        with conn_dst.cursor() as cur:
            cur.copy_expert(copy_sql, buffer)
            conn_dst.commit()

        rows_processed += len(rows)
        print(f"✅ Batch insertado: {len(rows)} filas")

    print(f"\n🟢 Total insertado: {rows_processed} filas")

except Exception as e:
    print(f"❌ Error durante el ETL: {e}")

finally:
    cursor_src.close()
    conn_src.close()
    conn_dst.close()
    print("🔌 Conexiones cerradas.")
