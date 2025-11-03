import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime
from io import StringIO
import psycopg2

# =========================
# 🔧 Configuración inicial
# =========================
load_dotenv()

pg_user_src = os.getenv("PG_USER2")
pg_pass_src = os.getenv("PG_PASS2")
pg_host_src = os.getenv("PG_HOST2")
pg_db_src   = os.getenv("PG_DB2")

pg_user_dst = os.getenv("PG_USER")
pg_pass_dst = os.getenv("PG_PASS")
pg_host_dst = os.getenv("PG_HOST")
pg_db_dst   = os.getenv("PG_DB")

print(f"📦 Base de datos origen: {pg_db_src}")
print(f"🎯 Base de datos destino: {pg_db_dst}")

# Conexiones
conn_src = psycopg2.connect(
    dbname=pg_db_src,
    user=pg_user_src,
    password=pg_pass_src,
    host=pg_host_src,
    port=5433
)
conn_dst = psycopg2.connect(
    dbname=pg_db_dst,
    user=pg_user_dst,
    password=pg_pass_dst,
    host=pg_host_dst,
    port=5433
)
engine_src = create_engine(f"postgresql+psycopg2://{pg_user_src}:{pg_pass_src}@{pg_host_src}:5433/{pg_db_src}")

# =========================
# 🕒 Control de fechas
# =========================
start_time = datetime.now()
print(f"\n🕒 Inicio del ETL: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

today = datetime.today()
current_year = today.year
current_month = today.month

if current_month == 1:
    year_target = current_year - 1
    month_target = 12
else:
    year_target = current_year
    month_target = current_month - 1

data_found = False

# =========================
# 🚀 Proceso principal
# =========================
try:
    for y in [year_target]:
        for m in range(1,8):  # Ajusta el rango según lo que quieras procesar
            month_str = f"{m:02d}"
            table_src = f"public.dw_consulta_externa_{y}_{month_str}"
            table_dst = f"dssge.dw_consulta_externa_{y}_{month_str}"

            print(f"\n📂 Procesando partición: {table_src} -> {table_dst}")
            start_month_time = datetime.now()

            # 1️⃣ Truncar la tabla destino
            with conn_dst.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {table_dst}")
                conn_dst.commit()
                print(f"🧹 Tabla destino {table_dst} truncada antes de la carga")

            # 2️⃣ Leer todos los datos del origen
            query = f"SELECT * FROM {table_src} ORDER BY anio"
            df = pd.read_sql_query(query, engine_src)

            if df.empty:
                print(f"⚠️ No se encontraron datos en {table_src}")
                continue

            data_found = True
            print(f"📥 Filas leídas desde {table_src}: {len(df)}")

            # 3️⃣ Normalizar nombres de columnas
            df.columns = df.columns.str.lower()

            # 4️⃣ Exportar a CSV en memoria
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False, header=False)
            csv_buffer.seek(0)

            # 5️⃣ Cargar en destino usando COPY
            with conn_dst.cursor() as cur:
                copy_sql = f"""
                    COPY {table_dst} ({', '.join(df.columns)})
                    FROM STDIN WITH CSV
                """
                cur.copy_expert(copy_sql, csv_buffer)
                conn_dst.commit()

            end_month_time = datetime.now()
            duration_min = (end_month_time - start_month_time).total_seconds() / 60
            print(f"✅ Carga completada: {len(df)} filas insertadas en {duration_min:.2f} minutos")

except Exception as e:
    print(f"❌ Error durante la ejecución del ETL: {e}")

finally:
    conn_src.close()
    conn_dst.close()
    print("🔌 Conexiones cerradas correctamente.")

end_time = datetime.now()
duration = end_time - start_time

print(f"\n🕓 Fin del ETL: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"⏱️ Tiempo total de procesamiento: {duration}")

if data_found:
    print("✅ ETL finalizado correctamente.")
else:
    print("⚠️ No se encontraron datos en ninguna partición.")
