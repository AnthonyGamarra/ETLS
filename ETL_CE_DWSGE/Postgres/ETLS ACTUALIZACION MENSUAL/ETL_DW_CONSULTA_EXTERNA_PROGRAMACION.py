import os
import psycopg2
import pandas as pd
import io
from dotenv import load_dotenv
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Cargar variables de entorno
load_dotenv()

# Configuración de base de datos origen y destino
pg_user = os.getenv("PG_USER")
pg_pass = os.getenv("PG_PASS")
pg_host = os.getenv("PG_HOST")
pg_db   = os.getenv("PG_DB")

# Crear conexiones
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

# Parámetros de fechas
##fecha_inicio = datetime(2025, 1, 1)  # Cambia esto si quieres otro inicio
##fecha_fin = datetime(2025, 12, 31)    # Cambia esto si quieres otro fin

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
print(f"\n🕒 Inicio del ETL: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

table_name = "dwsge.dwe_consulta_externa_programacion"
for mes in range(6,7):
    mes_str = f"{mes:02d}"
    try:
        cur_dst = conn_dst.cursor()
        partition_name = f"dwsge.dwe_consulta_externa_programacion_{anio}_{mes_str}"
        cur_dst.execute(f"TRUNCATE TABLE {partition_name};")
        conn_dst.commit()
        cur_dst.close()
        print(f"Partición truncada correctamente: {partition_name}")
    except Exception as e:
        print(f"Error truncando partición {partition_name}: {e}")  

    print(f"\n📆 Procesando mes: {anio}-{mes_str}")

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

        if df.empty:
            print(f"⚠️ No se encontraron datos para {anio}-{mes_str}")
        else:
            df.columns = df.columns.str.lower()
            table_name = "dwsge.dwe_consulta_externa_programacion"

            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            cur_dst = conn_dst.cursor()
            cols = ','.join(df.columns)
            copy_sql = f"COPY {table_name} ({cols}) FROM STDIN WITH CSV"

            cur_dst.copy_expert(sql=copy_sql, file=buffer)
            conn_dst.commit()
            cur_dst.close()

            print(f"✅ Carga completada para {anio}-{mes_str}: filas cargadas {len(df)}")

    except Exception as e:
        print(f"❌ Error en {anio}-{mes_str}: {e}")


# Cierre de conexiones
conn_src.close()
conn_dst.close()

end_time = datetime.now()
print(f"\n🕓 Proceso finalizado en {end_time - start_time}")