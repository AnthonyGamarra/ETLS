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

anio = datetime.now().year - 1
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

table_name = "DWH_SGE.DWE_CONSULTA_EXTERNA_CITADOS_HOMOLOGACION"


for mes in range(12,13):
    mes_str = f"{mes:02d}"

    # -------------------------------------------
    # Truncar partición Oracle
    # -------------------------------------------
    try:
        tabla = "DWH_SGE.DWE_CONSULTA_EXTERNA_CITADOS_HOMOLOGACION"
        partition_name = f"P{anio}_{mes_str}"

        sql_truncate = f"ALTER TABLE {tabla} TRUNCATE SUBPARTITION {partition_name}"

        with conn_dst.cursor() as cur:
            cur.execute(sql_truncate)
            conn_dst.commit()

        print(f"Partición {partition_name} truncada correctamente.")
    except Exception as e:
        print(f"Advertencia: error truncando partición {partition_name}: {e}")

    try:
        query = f"""
        SELECT
            d.anio,
            d.periodo,
            d.cod_oricentro,
            d.cod_centro,
            d.acto_med,
            d.cod_area,
            d.cod_servicio,    
            d.cod_actividad,
            d.cod_subactividad,
            d.cod_tipo_paciente as cod_paciente,
            e.cod_especialidad,
            e.cod_subespecialidad,
            e.cod_agrupador,
            e.cod_variable,
            d.cod_estado_cita as cod_estado,
            d.cod_tipo_cita,
            d.doc_paciente,    
            d.sexo,
            d.fecha_solicitud,
            d.fecha_creacion,
            d.fecha_cita,
            CASE 
                    WHEN (d.fecha_solicitud = '0001-01-01') 
                    OR (TO_DATE(d.fecha_solicitud,'YYYY-MM-DD') < TO_DATE(d.fecha_creacion,'YYYY-MM-DD') 
                        AND d.fecha_solicitud <> '0001-01-01')  
                    THEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_creacion,'YYYY-MM-DD'))
                    ELSE (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_solicitud,'YYYY-MM-DD'))
                END AS diferimiento,

                CASE 
                    WHEN d.fecha_solicitud = '0001-01-01' THEN
                        CASE
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_creacion,'YYYY-MM-DD')) < 7 THEN '1'
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_creacion,'YYYY-MM-DD')) BETWEEN 7 AND 10 THEN '2'
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_creacion,'YYYY-MM-DD')) BETWEEN 11 AND 30 THEN '3'
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_creacion,'YYYY-MM-DD')) BETWEEN 31 AND 60 THEN '4'
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_creacion,'YYYY-MM-DD')) BETWEEN 61 AND 90 THEN '5'
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_creacion,'YYYY-MM-DD')) BETWEEN 91 AND 120 THEN '6'
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_creacion,'YYYY-MM-DD')) BETWEEN 121 AND 150 THEN '7'
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_creacion,'YYYY-MM-DD')) > 150 THEN '8'
                            ELSE '0'
                        END
                    
                    WHEN TO_DATE(d.fecha_solicitud,'YYYY-MM-DD') < TO_DATE(d.fecha_creacion,'YYYY-MM-DD')
                        AND d.fecha_solicitud <> '0001-01-01' THEN
                        CASE
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_creacion,'YYYY-MM-DD')) < 7 THEN '1'
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_creacion,'YYYY-MM-DD')) BETWEEN 7 AND 10 THEN '2'
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_creacion,'YYYY-MM-DD')) BETWEEN 11 AND 30 THEN '3'
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_creacion,'YYYY-MM-DD')) BETWEEN 31 AND 60 THEN '4'
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_creacion,'YYYY-MM-DD')) BETWEEN 61 AND 90 THEN '5'
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_creacion,'YYYY-MM-DD')) BETWEEN 91 AND 120 THEN '6'
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_creacion,'YYYY-MM-DD')) BETWEEN 121 AND 150 THEN '7'
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_creacion,'YYYY-MM-DD')) > 150 THEN '8'
                            ELSE '0'
                        END

                    ELSE 
                        CASE
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_solicitud,'YYYY-MM-DD')) < 7 THEN '1'
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_solicitud,'YYYY-MM-DD')) BETWEEN 7 AND 10 THEN '2'
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_solicitud,'YYYY-MM-DD')) BETWEEN 11 AND 30 THEN '3'
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_solicitud,'YYYY-MM-DD')) BETWEEN 31 AND 60 THEN '4'
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_solicitud,'YYYY-MM-DD')) BETWEEN 61 AND 90 THEN '5'
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_solicitud,'YYYY-MM-DD')) BETWEEN 91 AND 120 THEN '6'
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_solicitud,'YYYY-MM-DD')) BETWEEN 121 AND 150 THEN '7'
                            WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') - TO_DATE(d.fecha_solicitud,'YYYY-MM-DD')) > 150 THEN '8'
                            ELSE '0'
                        END
                END AS dif_clasificado,

                d.anio_edad,

                CASE 
                    WHEN CAST(d.anio_edad AS INTEGER) <= 10 THEN '1'
                    WHEN CAST(d.anio_edad AS INTEGER) BETWEEN 11 AND 20 THEN '2'
                    WHEN CAST(d.anio_edad AS INTEGER) BETWEEN 21 AND 30 THEN '3'
                    WHEN CAST(d.anio_edad AS INTEGER) BETWEEN 31 AND 40 THEN '4'
                    WHEN CAST(d.anio_edad AS INTEGER) BETWEEN 41 AND 50 THEN '5'
                    WHEN CAST(d.anio_edad AS INTEGER) BETWEEN 51 AND 60 THEN '6'
                    WHEN CAST(d.anio_edad AS INTEGER) BETWEEN 61 AND 70 THEN '7'
                    WHEN CAST(d.anio_edad AS INTEGER) BETWEEN 71 AND 80 THEN '8'
                    WHEN CAST(d.anio_edad AS INTEGER) BETWEEN 81 AND 90 THEN '9'
                    WHEN CAST(d.anio_edad AS INTEGER) > 90 THEN '10'
                    ELSE '0'
                END AS edad_clasificado,

                CASE 
                    WHEN d.fecha_solicitud = '0001-01-01' THEN '2'
                    WHEN (TO_DATE(d.fecha_solicitud,'YYYY-MM-DD') < TO_DATE(d.fecha_creacion,'YYYY-MM-DD')) 
                        AND d.fecha_solicitud <> '0001-01-01' THEN '3'
                    WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') < TO_DATE(d.fecha_solicitud,'YYYY-MM-DD')) 
                    AND (TO_DATE(d.fecha_cita,'YYYY-MM-DD') < TO_DATE(d.fecha_creacion,'YYYY-MM-DD')) THEN '4'
                    WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') < TO_DATE(d.fecha_solicitud,'YYYY-MM-DD')) 
                    AND (TO_DATE(d.fecha_cita,'YYYY-MM-DD') > TO_DATE(d.fecha_creacion,'YYYY-MM-DD')) THEN '5'
                    WHEN (TO_DATE(d.fecha_cita,'YYYY-MM-DD') = TO_DATE(d.fecha_solicitud,'YYYY-MM-DD')) THEN '6'
                    ELSE '1'
                END AS flag_calidad,
                
                1 AS num_citas

            FROM dssge.sgss_ctcam10_m_{anio}_{mes_str} d
                LEFT JOIN dssge.dw_homologacion_enlaces e 
                        ON d.cod_actividad::text = e.cod_actividad::text 
                        AND d.cod_subactividad::text = e.cod_subactividad::text 
                        AND d.cod_servicio::text = e.cod_servicio::text
        """

        df = pd.read_sql_query(query, conn_src)

        if df.empty:
            print(f"No se encontraron datos para el mes {mes_str}")
        else:
            df.columns = df.columns.str.lower()

        # ======================================================
        # INSERTAR EN ORACLE USANDO executemany
        # ======================================================
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