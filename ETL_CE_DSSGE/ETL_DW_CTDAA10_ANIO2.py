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
# Para la base destino solo usamos psycopg2 directamente para COPY
conn_pg = psycopg2.connect(
    dbname=pg_db2,
    user=pg_user2,
    password=pg_pass2,
    host=pg_host2,
    port=5433
)

start_time = datetime.now()
print(f"\n🕒 Inicio del ETL: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

today = datetime.today()
current_year = today.year
current_month = today.month

# Calcular los dos meses anteriores según tu lógica
if current_month == 1:
    months_to_process = [(current_year - 1, 12)]
else:
    months_to_process = [(current_year, current_month - 1)]

table_name = "public.sgss_ctdaa10_anio"
data_found = False

try:
    for year, month in months_to_process:
        periodo = f"{year}{month:02d}"
        print(f"\n🔄 Procesando periodo: {periodo}")

        # DELETE previo para ese periodo en la tabla destino
        with conn_pg.cursor() as cur:
            delete_sql = f"TRUNCATE TABLE dssge.sgss_ctdaa10_anio_{year}_{month:02d}"
            cur.execute(delete_sql)
            conn_pg.commit()
            print(f"🗑️  Datos borrados para el periodo {periodo} en la tabla destino")

        # Leer todo el mes completo sin batching
        query = f"""
            SELECT * FROM {table_name}
            WHERE periodo = '{periodo}'
            ORDER BY anio
        """
        chunk_df = pd.read_sql_query(query, engine_pg2)

        if chunk_df.empty:
            print(f"⚠️  No se encontraron datos para el periodo {periodo} en la tabla origen")
            continue

        data_found = True
        print(f"📥 Filas leídas para el periodo {periodo}: {len(chunk_df)}")

        chunk_df.columns = chunk_df.columns.str.lower()

        # Exportar DataFrame a CSV en memoria (sin índice, sin encabezado)
        csv_buffer = StringIO()
        chunk_df.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)

        # Usar COPY para insertar rápido
        with conn_pg.cursor() as cur:
            copy_sql = f"""
                COPY dssge.sgss_ctdaa10_anio ({', '.join(chunk_df.columns)})
                FROM STDIN WITH CSV
            """
            cur.copy_expert(copy_sql, csv_buffer)
            conn_pg.commit()

        print(f"✅ Datos insertados para el periodo {periodo}")

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
    print("⚠️  No se encontraron datos en los periodos procesados.")