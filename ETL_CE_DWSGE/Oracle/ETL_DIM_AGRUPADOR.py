import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

# ============================
# 1. Cargar variables de entorno
# ============================
load_dotenv()

# ------------ ORIGEN: PostgreSQL ------------
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

# ============================
# 2. Crear conexión a PostgreSQL
# ============================
engine_src = create_engine(
    f"postgresql+psycopg2://{pg_user_src}:{pg_pass_src}@{pg_host_src}:5432/{pg_db_src}"
)

# ============================
# 3. Crear conexión a Oracle (SQLAlchemy)
# ============================
# Oracle via oracledb (modo thin)
engine_dst = create_engine(
    f"oracle+oracledb://{oracle_user}:{oracle_pass}@{oracle_host}:{oracle_port}/?service_name={oracle_service}"
)

# ============================
# 4. Registrar inicio
# ============================
start_time = datetime.now()
print(f"\n🕒 Inicio del proceso: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

try:
    # ============================
    # 5. Leer desde PostgreSQL
    # ============================
    query = "SELECT DISTINCT cod_agrupador, agrupador FROM public.homenlaces_historico_total"
    df = pd.read_sql(query, engine_src)

    if df.empty:
        print("⚠️  No se encontraron datos en el origen PostgreSQL.")
    else:

        # Limpieza previa del destino (si lo deseas, descomenta)
        # with engine_dst.begin() as conn:
        #     conn.execute(text("TRUNCATE TABLE DWH_SGE.DIM_AGRUPADOR"))

        # Normalizar columnas
        df.columns = df.columns.str.lower()

        # ============================
        # 6. Insertar en Oracle destino
        # ============================
        df.to_sql(
            name="dim_agrupador",
            con=engine_dst,
            schema="DWH_SGE",
            if_exists="append",
            index=False,
            dtype=None
        )

        print(f"✅ Datos cargados exitosamente en Oracle: {len(df)} filas")

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
