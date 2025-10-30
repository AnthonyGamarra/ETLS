import os
import oracledb
import pandas as pd
from sqlalchemy import create_engine,text
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
query = "SELECT * FROM SGSS.cmsho10"
df = pd.read_sql(query, conn_oracle)
conn_oracle.close()

# ==============================
# 4. Conexión a PostgreSQL
# ==============================
engine_pg = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:5433/{pg_db}")


with engine_pg.begin() as conn:
    # Limpia solo los datos, mantiene estructura e índices
    conn.execute(text("TRUNCATE TABLE dssge.sgss_cmsho10 RESTART IDENTITY;"))


# ==============================
# 5. Cargar datos a PostgreSQL
# ==============================
df = df.astype(str)
df.columns = df.columns.str.lower() 
df.to_sql("sgss_cmsho10", engine_pg, schema="dssge", if_exists="replace", index=False)



