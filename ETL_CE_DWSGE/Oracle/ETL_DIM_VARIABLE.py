import os
import pandas as pd
from sqlalchemy import create_engine,text
from dotenv import load_dotenv
from datetime import datetime

# Cargar variables de entorno
load_dotenv()

# Configuración de base de datos origen
pg_user_src = os.getenv("PG_USER3")
pg_pass_src = os.getenv("PG_PASS3")
pg_host_src = os.getenv("PG_HOST3")
pg_db_src   = os.getenv("PG_DB3")

# ------------ DESTINO: Oracle ------------
oracle_user = os.getenv("ORACLE_USER2")
oracle_pass = os.getenv("ORACLE_PASS2")
oracle_host = os.getenv("ORACLE_HOST2")
oracle_port = os.getenv("ORACLE_PORT2")
oracle_service = os.getenv("ORACLE_SERVICE2")  # Nombre del servicio Oracle

print(f"Base de datos origen: {pg_db_src}")
print(f"Base de datos destino (Oracle service): {oracle_service}")

# Crear conexiones
engine_src = create_engine(f"postgresql+psycopg2://{pg_user_src}:{pg_pass_src}@{pg_host_src}:5432/{pg_db_src}")
engine_dst = create_engine(
    f"oracle+oracledb://{oracle_user}:{oracle_pass}@{oracle_host}:{oracle_port}/?service_name={oracle_service}"
)
# Registrar inicio
start_time = datetime.now()
print(f"\n🕒 Inicio del proceso: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

#with engine_dst.begin() as conn:
    # Limpia solo los datos, mantiene estructura e índices
#    conn.execute(text("TRUNCATE TABLE DWH_SGE.dim_variable RESTART IDENTITY;"))

try:
    query = "SELECT DISTINCT cod_variable, variable FROM public.homenlaces_historico_total"
    df = pd.read_sql(query, engine_src)

    if df.empty:
        print("⚠️  No se encontraron datos en la tabla de origen.")
    else:
        df.columns = df.columns.str.lower()  # Normalizar columnas

        with engine_dst.begin() as conn:
            conn.execute(text("TRUNCATE TABLE DWH_SGE.DIM_VARIABLE"))
        df.to_sql(
            name="dim_variable",
            con=engine_dst,
            schema="DWH_SGE",
            if_exists="append",
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
