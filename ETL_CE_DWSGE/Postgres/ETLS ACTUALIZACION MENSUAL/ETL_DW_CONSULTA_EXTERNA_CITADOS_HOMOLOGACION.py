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
time= datetime.now().strftime("'%Y-%m-%d %H:%M:%S'")

for mes in range(5, 6): 
    mes_str = f"{mes:02d}"

    print(f"\nProcesando mes: {anio}-{mes_str}")
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
        actualizacion = f"""UPDATE dwsge.fecha_act
                            SET fecha_act = {time}
                            WHERE id=2"""
        df = pd.read_sql_query(query, conn_src)

        if df.empty:
            print(f"No se encontraron datos para el mes {mes_str}")
        else:
            df.columns = df.columns.str.lower()
            try:
                cur_dst = conn_dst.cursor()
                partition_name = f"dwsge.dwe_consulta_externa_citados_homologacion_{anio}_{mes_str}"
                cur_dst.execute(f"TRUNCATE TABLE {partition_name};")
                conn_dst.commit()
                cur_dst.close()
                print(f"Partición truncada correctamente: {partition_name}")
            except Exception as e:
                print(f"Error truncando partición {partition_name}: {e}")   

            # Truncar partición del mes antes de cargar datos

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
            cur_dst.execute(actualizacion)
            conn_dst.commit()
            cur_dst.close()

            print(f"Carga con COPY completada mes {mes_str}: filas totales cargadas {len(df)}")

    except Exception as e:
        print(f"Error en mes {mes_str}: {e}")

conn_src.close()
conn_dst.close()

end_time = datetime.now()
print(f"Proceso finalizado en {end_time - start_time}")