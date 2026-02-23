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

table_name = "dwsge.dwe_emergencia_atenciones_homologacion"
time= datetime.now().strftime("'%Y-%m-%d %H:%M:%S'")

for mes in range(2,3):
    mes_str = f"{mes:02d}"
    print(f"\nProcesando mes: {anio}-{mes_str}")
    try:
        query = f"""
                SELECT
                a.COD_ORICENTRO,
                a.COD_CENTRO,
                a.anio,
                a.periodo,
                a.COD_TOPICO,
                a.ACTO_MED,
                a.FECHA_ATEN,
                a.HORA_ATEN,
                a.COD_TIPO_PACIENTE,
                a.COD_PRIORIDAD,
                a.COD_DIAGNOSTICO,
                a.COD_EMERGENCIA,
                a.SECUEN_ATEN,
                z.cod_estandar
                from dssge.dw_ate_emer_{anio}_{mes_str} a
                LEFT JOIN dssge.dw_homologacion_enlaces_emergencia z
                                        ON z.cod_centro     = a.COD_CENTRO
                                        AND z.cod_topico     = a.COD_TOPICO
                                        AND z.cod_emergencia = a.COD_EMERGENCIA
                order by secuen_aten desc
        """
        actualizacion = f"""UPDATE dwsge.fecha_act
                            SET fecha_act = {time}
                            WHERE id=7"""
        df = pd.read_sql_query(query, conn_src)

        if df.empty:
            print(f"No se encontraron datos para el mes {mes_str}")
        else:
            df.columns = df.columns.str.lower()

            try:
                    cur_dst = conn_dst.cursor()
                    partition_name = f"dwsge.dwe_emergencia_atenciones_homologacion_{anio}_{mes_str}"
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