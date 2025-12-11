import os
import psycopg2
import pandas as pd
import oracledb
import io
from dotenv import load_dotenv
from datetime import datetime

# Cargar variables de entorno
load_dotenv()

# Configuración de base de datos origen (para pd.read_sql_query usamos psycopg2 connection)
pg_user_src = os.getenv("PG_USER")
pg_pass_src = os.getenv("PG_PASS")
pg_host_src = os.getenv("PG_HOST")
pg_db_src   = os.getenv("PG_DB")

# Configuración de base de datos destino
oracle_user2 = os.getenv("ORACLE_USER2")
oracle_pass2 = os.getenv("ORACLE_PASS2")
oracle_host2 = os.getenv("ORACLE_HOST2")
oracle_port2 = os.getenv("ORACLE_PORT2")
oracle_service2 = os.getenv("ORACLE_SERVICE2")

# Crear conexión origen (para leer datos)
conn_src = psycopg2.connect(
    dbname=pg_db_src,
    user=pg_user_src,
    password=pg_pass_src,
    host=pg_host_src,
    port=5433
)

# Crear conexión destino (para cargar datos)
dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={oracle_host2})(PORT={oracle_port2}))(CONNECT_DATA=(SERVICE_NAME={oracle_service2})))"

conn_dst = oracledb.connect(
    user=oracle_user2,
    password=oracle_pass2,
    dsn=dsn
)
conn_dst.autocommit = False

anio = datetime.now().year
start_time = datetime.now()
today = datetime.today()
current_year = today.year
current_month = today.month

# Calcular los dos meses anteriores
if current_month == 1:
    months_to_process = [(current_year - 1, 11), (current_year - 1, 12)]
elif current_month == 2:
    months_to_process = [(current_year - 1, 12), (current_year, 1)]
else:
    months_to_process = [(current_year, current_month - 2), (current_year, current_month - 1)]

print(f"\nInicio del ETL: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

table_name = "DWH_SGE.dwe_emergencia_defunciones_homologacion"


for mes in range(1,12):
    mes_str = f"{mes:02d}"

    try:
        query = f"""
                SELECT 
                t.deforicenasicod as cod_oricentro,
                t.defcenasicod as cod_centro,
                periodo,
                anio,
                deftipdocidenpercod cod_tip_doc,
                tipo_paciente as cod_tipo_paciente,
                defactmednum as acto_med,
                defanoedad as anio_def
                FROM dssge.sgss_ctdef10_{anio}_{mes_str} t
                WHERE
                t.defarehoscod = '02' AND
                t.defestregcod = '1'
        """

        df = pd.read_sql_query(query, conn_src)

        # -------------------------------------------
        # Truncar partición Oracle
        # -------------------------------------------
        try:
            tabla = "DWH_SGE.dwe_emergencia_defunciones_homologacion"
            partition_name = f"P{anio}_{mes_str}"

            sql_truncate = f"ALTER TABLE {tabla} TRUNCATE SUBPARTITION {partition_name}"

            with conn_dst.cursor() as cur:
                cur.execute(sql_truncate)
                conn_dst.commit()

            print(f"Partición {partition_name} truncada correctamente.")
        except Exception as e:
            print(f"Advertencia: error truncando partición {partition_name}: {e}")

        if df.empty:
            print(f"No se encontraron datos para el mes {mes_str}")
        else:
            df.columns = df.columns.str.lower()

        # ======================================================
        # INSERTAR EN ORACLE USANDO executemany
        # ======================================================
                # Cargar datos
        table_name = "DWH_SGE.dwe_emergencia_defunciones_homologacion"
        cols = list(df.columns)
        placeholders = ",".join([f":{i+1}" for i in range(len(cols))])
        sql_insert = f"INSERT INTO {table_name} ({','.join(cols)}) VALUES ({placeholders})"

        data = [tuple(row) for row in df.to_numpy()]

        with conn_dst.cursor() as cur:
            cur.executemany(sql_insert, data)
            conn_dst.commit()

        print(f"Filas cargadas en Oracle para {mes_str}: {len(df)}")

    except Exception as e:
        print(f"Error en mes {mes_str}: {e}")

conn_src.close()
conn_dst.close()

end_time = datetime.now()
print(f"Proceso finalizado en {end_time - start_time}")