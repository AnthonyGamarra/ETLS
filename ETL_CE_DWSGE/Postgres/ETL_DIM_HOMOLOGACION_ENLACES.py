import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime

# Cargar variables de entorno
load_dotenv()

# Configuración de base de datos origen
pg_user_src = os.getenv("PG_USER3")
pg_pass_src = os.getenv("PG_PASS3")
pg_host_src = os.getenv("PG_HOST3")
pg_db_src   = os.getenv("PG_DB3")

# Configuración de base de datos destino
pg_user_dst = os.getenv("PG_USER")
pg_pass_dst = os.getenv("PG_PASS")
pg_host_dst = os.getenv("PG_HOST")
pg_db_dst   = os.getenv("PG_DB")

print(f"Base de datos origen: {pg_db_src}")
print(f"Base de datos destino: {pg_db_dst}")

# Crear conexiones
engine_src = create_engine(f"postgresql+psycopg2://{pg_user_src}:{pg_pass_src}@{pg_host_src}:5432/{pg_db_src}")
engine_dst = create_engine(f"postgresql+psycopg2://{pg_user_dst}:{pg_pass_dst}@{pg_host_dst}:5433/{pg_db_dst}")

# Registrar inicio
start_time = datetime.now()
print(f"\n🕒 Inicio del proceso: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

try:
    query = "SELECT * FROM public.homenlaces_historico_total"
    df = pd.read_sql(query, engine_src)

    if df.empty:
        print("⚠️  No se encontraron datos en la tabla de origen.")
    else:
        df.columns = df.columns.str.lower()  # Normalizar columnas
        df.to_sql(
            name="dw_homologacion_enlaces",
            con=engine_dst,
            schema="dwsge",
            if_exists="replace",
            index=False
        )
        print(f"✅ Datos cargados exitosamente: {len(df)} filas")

except Exception as e:
    print(f"❌ Error durante la ejecución: {e}")

finally:
    engine_src.dispose()
    engine_dst.dispose()
    print("🔌 Conexiones cerradas.")

end_time = datetime.now()
duration = end_time - start_time
print(f"\n🕓 Fin del proceso: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"⏱️ Duración total: {duration}")
