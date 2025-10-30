import os
import psycopg2
import pandas as pd
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

# Crear conexiones (las abrimos una vez fuera del loop)
conn_src = psycopg2.connect(
    dbname=pg_db,
    user=pg_user,
    password=pg_pass,
    host=pg_host,
    port=5433
)

conn_dst = psycopg2.connect(
    dbname=pg_db,
    user=pg_user,
    password=pg_pass,
    host=pg_host,
    port=5433
)

anio = datetime.now().year
start_time = datetime.now()
print(f"\n🕒 Inicio del ETL: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
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

for anio, mes in months_to_process:
    mes_str = f"{mes:02d}"
    try:
        cur_dst = conn_dst.cursor()
        partition_name = f"dwsge.dwe_consulta_externa_horas_efectivas_{anio}_{mes_str}"
        cur_dst.execute(f"TRUNCATE TABLE {partition_name};")
        conn_dst.commit()
        cur_dst.close()
        print(f"Partición truncada correctamente: {partition_name}")
    except Exception as e:
        print(f"Error truncando partición {partition_name}: {e}")  

    print(f"\n📆 Procesando mes: {anio}-{mes_str}")
    
    try:
        query = f"""
            WITH atenciones AS (
                SELECT
                    u.cod_oricentro,
                    u.cod_centro,
                    u.cod_area,
                    u.cod_servicio,
                    u.cod_actividad,
                    u.cod_subactividad,
                    u.cod_tipdoc_medico,
                    u.cod_doc_medico,
                    u.fecha_cita,
                    u.horaini,
                    u.horafin,
                    COUNT(*) AS ate
                FROM 
                    dssge.sgss_ctcam10_m u
                WHERE 
                    u.cod_estado_cita = '4'
                GROUP BY
                    u.cod_oricentro,
                    u.cod_centro,
                    u.cod_area,
                    u.cod_servicio,
                    u.cod_actividad,
                    u.cod_subactividad,
                    u.cod_tipdoc_medico,
                    u.cod_doc_medico,
                    u.fecha_cita,
                    u.horaini,
                    u.horafin
            )

            SELECT b.*
            FROM (
                SELECT
                    t.anio,
                    t.periodo,
                    t.cenasicod AS cod_centro,
                    t.arehoscod AS cod_area,
                    t.actcod AS cod_actividad,
                    t.actespcod AS cod_subactividad,
                    t.servhoscod AS cod_servicio,
                    a.ate,
                    CASE
                        WHEN CAST(t.properturhorfin AS time) >= CAST(t.properturhorini AS time) THEN
                            EXTRACT(EPOCH FROM (CAST(t.properturhorfin AS time) - CAST(t.properturhorini AS time))) / 3600
                        ELSE
                            (EXTRACT(EPOCH FROM (CAST(t.properturhorfin AS time) + INTERVAL '24 hours' - CAST(t.properturhorini AS time))) / 3600)
                    END AS hras_prog,
                    t.estprogcitcod AS estado_progcita
                FROM 
                    dssge.sgss_ctppe10_{anio}_{mes_str} t
                LEFT JOIN dssge.sgss_cmprs10 pr 
                    ON pr.tipdocidenpercod = t.tipdocidenpercod
                    AND pr.perasisdocidennum = t.perasisdocidennum
                LEFT JOIN atenciones a 
                    ON t.propertipoprogperscod = '1'
                    AND a.cod_oricentro     = t.oricenasicod
                    AND a.cod_centro        = t.cenasicod
                    AND a.cod_area          = t.arehoscod
                    AND a.cod_servicio      = t.servhoscod
                    AND a.cod_actividad     = t.actcod
                    AND a.cod_subactividad  = t.actespcod
                    AND a.cod_tipdoc_medico = t.tipdocidenpercod
                    AND a.cod_doc_medico    = t.perasisdocidennum
                    AND a.fecha_cita        = t.properfec
                    AND a.horaini           = t.properturhorini
                    AND a.horafin           = t.properturhorfin
                WHERE 
                    t.oricenasicod IN ('1','2','3','4','5','6','7')
                    AND pr.grupocupcod = '01'
                    AND t.arehoscod = '01'
                    AND t.actcod = '91'
                    AND t.estprogcitcod IN ('2','4')
                    AND t.actespcod <> '092'

            ) b
            WHERE b.ate <> 0"""

        # Leer datos
        df = pd.read_sql_query(query, conn_src)

        if df.empty:
            print(f"⚠️ No se encontraron datos para {anio}-{mes_str}")
        else:
            df.columns = df.columns.str.lower()
            table_name = "dwsge.dwe_consulta_externa_horas_efectivas"

            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            with conn_dst.cursor() as cur_dst:
                cols = ','.join(df.columns)
                copy_sql = f"COPY {table_name} ({cols}) FROM STDIN WITH CSV"
                cur_dst.copy_expert(sql=copy_sql, file=buffer)
                conn_dst.commit()

            print(f"✅ Carga completada para {anio}-{mes_str}: filas cargadas {len(df)}")

    except Exception as e:
        print(f"❌ Error en {anio}-{mes_str}: {e}")

# Cierre de conexiones
conn_src.close()
conn_dst.close()

end_time = datetime.now()
print(f"\n🕓 Proceso finalizado en {end_time - start_time}")
