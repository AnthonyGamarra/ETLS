import os
import psycopg2
import pandas as pd
import oracledb
import io
from dotenv import load_dotenv
from datetime import datetime


# Cargar variables de entorno
load_dotenv()

# Configuración de base de datos origen y destino
pg_user = os.getenv("PG_USER")
pg_pass = os.getenv("PG_PASS")
pg_host = os.getenv("PG_HOST")
pg_db   = os.getenv("PG_DB")

oracle_user2 = os.getenv("ORACLE_USER2")
oracle_pass2 = os.getenv("ORACLE_PASS2")
oracle_host2 = os.getenv("ORACLE_HOST2")
oracle_port2 = os.getenv("ORACLE_PORT2")
oracle_service2 = os.getenv("ORACLE_SERVICE2")


# Crear conexiones (las abrimos una vez fuera del loop)
conn_src = psycopg2.connect(
    dbname=pg_db,
    user=pg_user,
    password=pg_pass,
    host=pg_host,
    port=5433
)

# =============================
# Conexión Oracle DESTINO
# =============================
dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={oracle_host2})(PORT={oracle_port2}))(CONNECT_DATA=(SERVICE_NAME={oracle_service2})))"

conn_dst = oracledb.connect(
    user=oracle_user2,
    password=oracle_pass2,
    dsn=dsn
)
conn_dst.autocommit = False

# Parámetros de fechas
##fecha_inicio = datetime(2025, 1, 1)  # Cambia esto si quieres otro inicio
##fecha_fin = datetime(2025, 12, 31)    # Cambia esto si quieres otro fin

anio = datetime.now().year -1
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
print(f"\n🕒 Inicio del ETL: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

table_name = "DWH_SGE.dwe_consulta_externa_programacion_homologacion"
for mes in range(12,13):
    mes_str = f"{mes:02d}"

    try:
        query = f"""
                    SELECT 
                        a.anio as ANIO,
                        a.periodo as PERIODO,
                        a.oricenasicod as COD_ORICENTRO,
                        a.cenasicod as COD_CENTRO,
                        a.tipdocidenpercod as cod_tipdoc_medico,
	                    a.perasisdocidennum as dni_medico,
                        a.actcod as COD_ACTIVIDAD,
                        a.actespcod as COD_SUBACTIVIDAD,
                        a.servhoscod as COD_SERVICIO,
                        a.properprohortot as TOTAL_HORAS,
                        a.estprogcitcod as COD_ESTADO_PROGRAMACION,
                        e.cod_especialidad,
                        e.cod_subespecialidad,
                        e.cod_agrupador,
                        e.cod_variable,
                        a.motsusprogcod as COD_MOTIVO_SUSPENSION
                    FROM dssge.sgss_ctppe10_{anio}_{mes_str} a 
                    LEFT OUTER JOIN dssge.sgss_cmprs10 m 
                        ON m.tipdocidenpercod = a.tipdocidenpercod
                        AND m.perasisdocidennum = a.perasisdocidennum
                    LEFT JOIN dssge.dw_homologacion_enlaces e 
                        ON a.actcod::text = e.cod_actividad::text 
                        AND a.actespcod::text = e.cod_subactividad::text 
                        AND a.servhoscod::text = e.cod_servicio::text
                    LEFT OUTER JOIN dssge.sgss_cbgoc10 z 
                        ON m.grupocupcod = z.grupocupcod
                    WHERE a.oricenasicod IN ('1','2','3','4','5','6','7')
                        AND a.properestregcod = '1'
                        AND a.actcod = '91'
                        AND a.arehoscod = '01'
                        AND z.grupocupcod = '01'
                        AND a.estprogcitcod IN ('2','4')
                    ORDER BY PERIODO ASC"""

        # Leer datos
        df = pd.read_sql_query(query, conn_src)

        # -------------------------------------------
        # Truncar partición Oracle
        # -------------------------------------------
        try:
            tabla = "DWH_SGE.dwe_consulta_externa_programacion_homologacion"
            partition_name = f"P{anio}_{mes_str}"

            sql_truncate = f"ALTER TABLE {tabla} TRUNCATE SUBPARTITION {partition_name}"

            with conn_dst.cursor() as cur:
                cur.execute(sql_truncate)
                conn_dst.commit()

            print(f"Partición {partition_name} truncada correctamente.")
        except Exception as e:
            print(f"Advertencia: error truncando partición {partition_name}: {e}")

        if df.empty:
            print(f"⚠️ No se encontraron datos para {anio}-{mes_str}")
        else:
            
            df.columns = df.columns.str.lower()
            table_name = "DWH_SGE.dwe_consulta_externa_programacion_homologacion"

            # ======================================================
            # INSERTAR EN ORACLE USANDO executemany
            # ======================================================
                    # Cargar datos
            table_name = "DWH_SGE.dwe_consulta_externa_programacion_homologacion"
            cols = list(df.columns)
            placeholders = ",".join([f":{i+1}" for i in range(len(cols))])
            sql_insert = f"INSERT INTO {table_name} ({','.join(cols)}) VALUES ({placeholders})"

            data = [tuple(row) for row in df.to_numpy()]

            with conn_dst.cursor() as cur:
                cur.executemany(sql_insert, data)
                conn_dst.commit()

            print(f"Filas cargadas en Oracle para {mes_str}: {len(df)}")
    except Exception as e:
        print(f"❌ Error en {anio}-{mes_str}: {e}")


# Cierre de conexiones
conn_src.close()
conn_dst.close()

end_time = datetime.now()
print(f"\n🕓 Proceso finalizado en {end_time - start_time}")