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
print(f"\nInicio del ETL: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

# Obtener fecha actual
now = datetime.now()

# Mes actual
year_current = now.year
month_current = now.month

# Mes anterior (manejar cambio de año)
if month_current == 1:
    year_previous = year_current - 1
    month_previous = 12
else:
    year_previous = year_current
    month_previous = month_current - 1

months_to_process = [
    (year_previous, month_previous),
    (year_current, month_current)
]

print(f"Meses a procesar: {months_to_process}")

try:
    for year, month in months_to_process:
        month_str = f"{month:02d}"
        table_name = f"public.dw_consulta_externa_{year}_{month_str}"
        
        offset = 0
        while True:
            query = f"""
                SELECT * FROM {table_name}
                ORDER BY anio
                LIMIT {chunksize} OFFSET {offset}
            """
            chunk_df = pd.read_sql_query(query, engine_pg2)
            
            if chunk_df.empty:
                if offset == 0:
                    print(f"No se encontraron datos en la partición {table_name}")
                break
            
            data_found = True
            print(f"📥 Leyendo batch de {table_name} con OFFSET {offset}, filas leídas: {len(chunk_df)}")

            chunk_df.columns = chunk_df.columns.str.lower()

            csv_buffer = StringIO()
            chunk_df.to_csv(csv_buffer, index=False, header=False)
            csv_buffer.seek(0)

            with conn_pg.cursor() as cur:
                copy_sql = f"""
                    COPY dssge.dw_consulta_externa ({', '.join(chunk_df.columns)})
                    FROM STDIN WITH CSV
                """
                cur.copy_expert(copy_sql, csv_buffer)
                conn_pg.commit()

            print(f"Insertadas filas batch de {table_name} con OFFSET {offset}: {len(chunk_df)}")

            offset += chunksize

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
    print("ETL finalizado correctamente.")
else:
    print("No se encontraron datos en ninguna partición.")

