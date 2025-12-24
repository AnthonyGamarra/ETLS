import os
import oracledb
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from sqlalchemy.dialects.oracle import VARCHAR2

load_dotenv()

# ---------- Oracle origen ----------
oracle_user = os.getenv("ORACLE_USER")
oracle_pass = os.getenv("ORACLE_PASS")
oracle_host = os.getenv("ORACLE_HOST")
oracle_port = os.getenv("ORACLE_PORT")
oracle_service = os.getenv("ORACLE_SERVICE")

dsn_origen = f"{oracle_host}:{oracle_port}/{oracle_service}"
conn_oracle = oracledb.connect(
    user=oracle_user,
    password=oracle_pass,
    dsn=dsn_origen
)

df = pd.read_sql("SELECT * FROM SGSS.mbrai10", conn_oracle)
conn_oracle.close()

# ---------- Oracle destino ----------
oracle_user2 = os.getenv("ORACLE_USER2")
oracle_pass2 = os.getenv("ORACLE_PASS2")
oracle_host2 = os.getenv("ORACLE_HOST2")
oracle_port2 = os.getenv("ORACLE_PORT2")
oracle_service2 = os.getenv("ORACLE_SERVICE2")

engine_oracle_dest = create_engine(
    f"oracle+oracledb://{oracle_user2}:{oracle_pass2}@{oracle_host2}:{oracle_port2}/?service_name={oracle_service2}"
)

# ---------- Enviar DF a Oracle destino ----------
df.columns = df.columns.str.lower()
df = df.astype(str)

dtype_mapping = {col: VARCHAR2(500) for col in df.columns}

df.to_sql(
    "sgss_mbrai10",
    engine_oracle_dest,
    schema="DWH_SGE",
    if_exists="replace",
    index=False,
    dtype=dtype_mapping
)






