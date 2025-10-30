import os
import oracledb
import pandas as pd
from sqlalchemy import create_engine,text,inspect
from dotenv import load_dotenv

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
pg_db   = os.getenv("PG_DB")

# ==============================
# 2. Conexión a Oracle
# ==============================
dsn = f"{oracle_host}:{oracle_port}/{oracle_service}"
conn_oracle = oracledb.connect(user=oracle_user, password=oracle_pass, dsn=dsn)

# ==============================
# 3. Extraer datos de Oracle
# ==============================
query = "SELECT * FROM SGSS.MBEGP10"
df = pd.read_sql(query, conn_oracle)
conn_oracle.close()

# ==============================
# 4. Conexión a PostgreSQL
# ==============================
engine_pg = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:5433/{pg_db}")


# ==============================
# 5. Verificar si la tabla existe
# ==============================
table_name = "sgss_mbegp10"
schema_name = "dssge"

with engine_pg.begin() as conn:
    inspector = inspect(conn)
    tables = inspector.get_table_names(schema=schema_name)

    if table_name in tables:
        conn.execute(text(f"TRUNCATE TABLE {schema_name}.{table_name} RESTART IDENTITY;"))
        print(f"Tabla {schema_name}.{table_name} truncada correctamente.")
    else:
        print(f"Tabla {schema_name}.{table_name} no existe. Será creada automáticamente.")

# ==============================
# 5. Cargar datos a PostgreSQL
# ==============================
df = df.astype(str)
df.columns = df.columns.str.lower() 
df.to_sql("sgss_mbegp10", engine_pg, schema="dssge", if_exists="replace", index=False)



