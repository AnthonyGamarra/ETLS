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

table_name = "dssge.dwe_consulta_externa_homologacion"


for mes in range(1,11):
    mes_str = f"{mes:02d}"
    try:
        cur_dst = conn_dst.cursor()
        partition_name = f"dssge.dw_consulta_externa_homologacion_{anio}_{mes_str}"
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
            WITH max_orden AS (
                SELECT 
                    atenamboricenasicod,
                    atenambcenasicod,
                    atenambnum,
                    MAX(atenambdiagord::NUMERIC) AS max_orden
                FROM dssge.sgss_ctdaa10_anio_v2_{anio}_{mes_str}
                GROUP BY 
                    atenamboricenasicod,
                    atenambcenasicod,
                    atenambnum
            ),
            incluido_z AS (
                SELECT 
                    d.atenamboricenasicod,
                    d.atenambcenasicod,
                    d.atenambnum
                FROM dssge.sgss_ctdaa10_anio_v2_{anio}_{mes_str} d
                JOIN max_orden m
                    ON d.atenamboricenasicod = m.atenamboricenasicod
                AND d.atenambcenasicod = m.atenambcenasicod
                AND d.atenambnum = m.atenambnum
                WHERE d.atenambdiagord = 1
                AND (d.diagcod LIKE 'Z75.%' OR d.diagcod LIKE 'Z53.%')
                AND m.max_orden > 1
                GROUP BY 
                    d.atenamboricenasicod,
                    d.atenambcenasicod,
                    d.atenambnum
                HAVING COUNT(*) = 1
            ),
            base AS (
                SELECT 
                    a.cod_oricentro,
                    a.cod_centro,
                    a.periodo,
                    a.cod_servicio,
                    a.cod_actividad,
                    a.cod_subactividad,
                    a.dni_medico,
                    a.doc_paciente,
                    a.anio_edad,
                    a.anio,
                    a.meses,
                    a.sexo,
                    a.h_c,
                    a.cod_tip_seguro,
                    a.cod_tipo_parentesco,
                    a.cod_tipo_paciente,
                    a.cod_tipdoc_paciente,
                    a.fecha_solic,
                    a.fecha_cita,
                    a.fecha_atencion,
                    a.acto_med,
                    a.cod_tipo_consulta,
                    e.cod_especialidad,
                    e.cod_subespecialidad,
                    e.cod_agrupador,
                    e.cod_variable,
                    i.diagcod AS cod_diag,
                    a.total_horas
                FROM dssge.dw_consulta_externa_{anio}_{mes_str} a
                LEFT JOIN dssge.sgss_ctdaa10_anio_v2_{anio}_{mes_str} i 
                    ON a.cod_oricentro = i.atenamboricenasicod
                    AND a.cod_centro = i.atenambcenasicod
                    AND a.acto_med = i.atenambnum
                    AND i.atenambdiagord = 1
                LEFT JOIN dssge.dw_homologacion_enlaces e 
                    ON a.cod_actividad::text = e.cod_actividad::text 
                    AND a.cod_subactividad::text = e.cod_subactividad::text 
                    AND a.cod_servicio::text = e.cod_servicio::text
            )
            , analitico AS (
                SELECT 
                    b.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY b.cod_oricentro, b.cod_centro, b.cod_subactividad, 
                                    b.dni_medico, b.doc_paciente, b.acto_med, 
                                    b.fecha_atencion, b.cod_variable
                        ORDER BY b.cod_diag ASC
                    ) AS row_num,

                    COUNT(*) OVER (
                        PARTITION BY b.cod_oricentro, b.cod_centro, b.cod_subactividad, 
                                    b.dni_medico, b.doc_paciente, b.acto_med, 
                                    b.fecha_atencion, b.cod_variable
                    ) AS total_en_particion
                FROM base b
            )
            SELECT 
                a.*,
                CASE 
                    WHEN a.cod_diag IS NULL OR a.cod_diag = '' THEN 0

                    WHEN z.atenamboricenasicod IS NOT NULL THEN 6

                    WHEN a.total_en_particion = 1 
                        AND (a.cod_diag LIKE 'Z75.%' OR a.cod_diag LIKE 'Z53.%') THEN 1

                    WHEN a.total_en_particion = 1 THEN 2

                    WHEN a.row_num = 1 
                        AND (a.cod_diag LIKE 'Z75.%' OR a.cod_diag LIKE 'Z53.%') THEN 3

                    WHEN a.row_num = 1 THEN 4

                    ELSE 5
                END AS clasificacion

            FROM analitico a
            LEFT JOIN incluido_z z
                ON a.cod_oricentro = z.atenamboricenasicod
                AND a.cod_centro = z.atenambcenasicod
                AND a.acto_med = z.atenambnum;
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