import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime
from io import StringIO
import psycopg2

load_dotenv()

pg_user = os.getenv("PG_USER2")
pg_pass = os.getenv("PG_PASS2")
pg_host = os.getenv("PG_HOST2")
pg_db   = os.getenv("PG_DB2")

pg_user2 = os.getenv("PG_USER")
pg_pass2 = os.getenv("PG_PASS")
pg_host2 = os.getenv("PG_HOST")
pg_db2   = os.getenv("PG_DB")

print(f"Base de datos origen: {pg_db}")
print(f"Base de datos destino: {pg_db2}")

engine_pg2 = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:5433/{pg_db}")
conn_pg = psycopg2.connect(
    dbname=pg_db2,
    user=pg_user2,
    password=pg_pass2,
    host=pg_host2,
    port=5433
)

chunksize = 500000
data_found = False

start_time = datetime.now()
print(f"\n🕒 Inicio del ETL: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

try:
    table_name = "public.personas_essi_4"
    start_time_process = datetime.now()

    offset = 0
    rows_processed = 0

    while True:
        query = f"""
            SELECT * FROM {table_name}
            ORDER BY persecnum
            LIMIT {chunksize} OFFSET {offset}
        """
        chunk_df = pd.read_sql_query(query, engine_pg2)

        if chunk_df.empty:
            if offset == 0:
                print(f"⚠️  No se encontraron datos en la tabla {table_name}")
            break

        data_found = True
        print(f"📥 Leyendo batch de {table_name} con OFFSET {offset}, filas leídas: {len(chunk_df)}")

        chunk_df.columns = chunk_df.columns.str.lower()

        # Exportar DataFrame a CSV en memoria (sin índice, sin encabezado)
        csv_buffer = StringIO()
        chunk_df.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)

        # Usar COPY para insertar rápido
        with conn_pg.cursor() as cur:
            copy_sql = f"""
                COPY dssge.dwe_personas_essi ({', '.join(chunk_df.columns)})
                FROM STDIN WITH CSV
            """
            cur.copy_expert(copy_sql, csv_buffer)
            conn_pg.commit()

        print(f"✅ Insertadas filas batch de {table_name} con OFFSET {offset}: {len(chunk_df)}")
        rows_processed += len(chunk_df)

        offset += chunksize

    end_time_process = datetime.now()
    duration = (end_time_process - start_time_process).total_seconds() / 60  # minutos

    if rows_processed > 0:
        print(f"🕒 Tiempo total de ejecución para {table_name}: {duration:.2f} minutos\n")

except Exception as e:
    print(f"❌ Error durante la ejecución del ETL: {e}")

finally:
    engine_pg2.dispose()
    conn_pg.close()
    print("🔌 Conexiones cerradas.")

end_time = datetime.now()
print(f"\n🕓 Fin del ETL: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

duration = end_time - start_time
print(f"⏱️ Tiempo total de procesamiento: {duration}")

if data_found:
    print("✅ ETL finalizado correctamente.")
else:
    print("⚠️  No se encontraron datos en ninguna partición.")
