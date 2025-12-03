import os
import psycopg2
import pandas as pd
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
pg_user_dst = os.getenv("PG_USER")
pg_pass_dst = os.getenv("PG_PASS")
pg_host_dst = os.getenv("PG_HOST")
pg_db_dst   = os.getenv("PG_DB")

# Crear conexión origen (para leer datos)
conn_src = psycopg2.connect(
    dbname=pg_db_src,
    user=pg_user_src,
    password=pg_pass_src,
    host=pg_host_src,
    port=5433
)

# Crear conexión destino (para cargar datos)
conn_dst = psycopg2.connect(
    dbname=pg_db_dst,
    user=pg_user_dst,
    password=pg_pass_dst,
    host=pg_host_dst,
    port=5433
)

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

table_name = "dwsge.dwe_consulta_externa_citados_homologacion"


for mes in range(10,12):
    mes_str = f"{mes:02d}"
    try:
        cur_dst = conn_dst.cursor()
        partition_name = f"dwsge.dwe_consulta_externa_citados_homologacion_{anio}_{mes_str}"
        cur_dst.execute(f"TRUNCATE TABLE {partition_name};")
        conn_dst.commit()
        cur_dst.close()
        print(f"Partición truncada correctamente: {partition_name}")
    except Exception as e:
        print(f"Error truncando partición {partition_name}: {e}")   
    print(f"\nProcesando mes: {anio}-{mes_str}")
    # Truncar partición del mes antes de cargar datos

    try:
        query = f"""
        SELECT
            a.anio,
            a.cod_oricentro,
            a.cod_centro,
            a.acto_med,
            a.cod_area,
            a.cod_servicio,
            a.cod_actividad,
            a.cod_subactividad,
            a.periodo,
            a.cod_estado_cita as cod_estado,
            a.cod_tipo_paciente as cod_paciente,
            e.cod_especialidad,
            e.cod_subespecialidad,
            e.cod_agrupador,
            e.cod_variable
        FROM dssge.sgss_ctcam10_m_{anio}_{mes_str} a
        LEFT JOIN dssge.dw_homologacion_enlaces e 
            ON a.cod_actividad::text = e.cod_actividad::text 
            AND a.cod_subactividad::text = e.cod_subactividad::text 
            AND a.cod_servicio::text = e.cod_servicio::text 
        WHERE a.cod_actividad ='91'    

        """

        df = pd.read_sql_query(query, conn_src)

        if df.empty:
            print(f"No se encontraron datos para el mes {mes_str}")
        else:
            df.columns = df.columns.str.lower()

            # Exportar DataFrame a CSV en memoria
            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            # Usar cursor para cargar datos con COPY
            cur_dst = conn_dst.cursor()
            cols = ','.join(df.columns)
            copy_sql = f"COPY {table_name} ({cols}) FROM STDIN WITH CSV"

            cur_dst.copy_expert(sql=copy_sql, file=buffer)
            conn_dst.commit()
            cur_dst.close()

            print(f"Carga con COPY completada mes {mes_str}: filas totales cargadas {len(df)}")

    except Exception as e:
        print(f"Error en mes {mes_str}: {e}")

conn_src.close()
conn_dst.close()

end_time = datetime.now()
print(f"Proceso finalizado en {end_time - start_time}")