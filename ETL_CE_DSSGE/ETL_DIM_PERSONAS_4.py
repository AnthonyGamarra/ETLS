import os
import pandas as pd
import psycopg2
from io import StringIO
from dotenv import load_dotenv
from datetime import datetime

# ==============================
# 1. Cargar variables desde .env
# ==============================
load_dotenv()

# Origen PostgreSQL
pg_user_src = os.getenv("PG_USER")
pg_pass_src = os.getenv("PG_PASS")
pg_host_src = os.getenv("PG_HOST")
pg_port_src = os.getenv("PG_PORT", '5433')
pg_db_src   = os.getenv("PG_DB")

# Destino PostgreSQL
pg_user_dst = os.getenv("PG_USER2")
pg_pass_dst = os.getenv("PG_PASS2")
pg_host_dst = os.getenv("PG_HOST2")
pg_port_dst = os.getenv("PG_PORT2", '5433')
pg_db_dst   = os.getenv("PG_DB2")

print(f"Origen POSTGRES:    {pg_db_src}")
print(f"Destino POSTGRES:   {pg_db_dst}")

# ==============================
# 2. Conexión ORIGEN PostgreSQL
# ==============================
conn_src = psycopg2.connect(
    dbname=pg_db_src,
    user=pg_user_src,
    password=pg_pass_src,
    host=pg_host_src,
    port=pg_port_src
)

# ==============================
# 3. Conexión DESTINO PostgreSQL
# ==============================
conn_dst = psycopg2.connect(
    dbname=pg_db_dst,
    user=pg_user_dst,
    password=pg_pass_dst,
    host=pg_host_dst,
    port=pg_port_dst
)

chunksize = 300000
data_found = False

start_time = datetime.now()
print(f"\n🕒 Inicio del ETL: {start_time:%Y-%m-%d %H:%M:%S}")

try:
    table_name = "dssge.dwe_personas_essi"

    cursor_src = conn_src.cursor()
    cursor_src.arraysize = chunksize

    sql_src = f"""
        SELECT 
            ROW_NUMBER() OVER (
                PARTITION BY p.pertipdocidencod, p.perdocidennum 
                ORDER BY to_date(p.perinsfec, 'DD/MM/YYYY') DESC
            ) AS row_num,
            *
        FROM {table_name} p
    """

    cursor_src.execute(sql_src)

    rows_processed = 0
    batch_n = 1

    while True:
        rows = cursor_src.fetchmany(chunksize)
        if not rows:
            if rows_processed == 0:
                print(f"⚠️ No se encontraron datos en el origen ({table_name})")
            break

        data_found = True

        colnames = [col[0].lower() for col in cursor_src.description]

        print(f"📥 Lote {batch_n}: filas leídas {len(rows)}")

        df = pd.DataFrame(rows, columns=colnames)

        # --- LIMPIEZA ---
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace('\x00', '', regex=False)
                .str.encode('utf-8', 'ignore')
                .str.decode('utf-8')
            )

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)

        copy_sql = f"""
            COPY public.personas_essi_4 ({', '.join(colnames)})
            FROM STDIN WITH CSV
        """

        with conn_dst.cursor() as cur:
            cur.copy_expert(copy_sql, csv_buffer)
        conn_dst.commit()

        print(f"✅ Insertadas {len(rows)} filas en Postgres")

        rows_processed += len(rows)
        batch_n += 1

    print(f"\nTotal filas procesadas: {rows_processed}")

except Exception as e:
    print(f"❌ ERROR EN ETL: {e}")

finally:
    try:
        conn_src.close()
        conn_dst.close()
    except:
        pass
    print("\n🔌 Conexiones cerradas.")

end_time = datetime.now()
print(f"🕓 Fin del ETL: {end_time:%Y-%m-%d %H:%M:%S}")
print(f"⏱ Tiempo total: {end_time - start_time}")

if data_found:
    print("✅ ETL POSTGRES → POSTGRES finalizado correctamente.")
else:
    print("⚠️ No se encontraron datos.")
