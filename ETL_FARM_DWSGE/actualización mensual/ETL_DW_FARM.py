import os
import psycopg2
import pandas as pd
import io
from dotenv import load_dotenv
from datetime import datetime

# Cargar variables de entorno
load_dotenv()

# Configuración de base de datos origen (para leer datos)
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

table_name = "dwsge.dwe_farm"
time = datetime.now().strftime("'%Y-%m-%d %H:%M:%S'")
chunk_size = 500000  # filas por bloque (farmacia tiene ~8-9 millones de filas por mes)

for mes in range(1, 11):
    mes_str = f"{mes:02d}"
    print(f"\nProcesando mes: {anio}-{mes_str}")
    try:
        query = f"""
                SELECT
                t.anio,
                t.periodo,
                t.cod_centro,
                t.cod_oricentro,
                t.cod_area,
                t.cod_topico,
                t.cod_servicio,
                t.cod_actividad,
                t.cod_subactividad,
                t.num_actomed,
                t.dni_profesional,
                t.cmp,
                t.fecha_solicitud,
                t.num_receta,
                t.cod_medicamento,
                t.cant_solicitud,
                t.cant_atendida,
                t.unidad,
                t.duracion_med,
                t.cod_dx,
                t.autogenerado,
                t.dni,
                t.sexo,
                t.edad_anio,
                t.meses,
                t.dias,
                t.cod_farmacia,
                t.codusu_despacho,
                t.cod_grpocupacional_desp,
                t.fecha_despacho,
                t.hora_despacho,
                t.numrecetmanual,
                t.cod_estacion,
                t.precio,
                t.cod_tipo_seguro,
                t.cod_tipo_parentesco,
                t.cod_tipo_paciente,
                t.cod_prioridad,
                t.tipo_movimiento,
                e.cod_especialidad,
                e.cod_subespecialidad,
                e.cod_agrupador,
                e.cod_variable
                from dssge.dw_farm_{anio}_{mes_str} t
                LEFT JOIN dssge.dw_homologacion_enlaces e
                    ON t.cod_actividad::text    = e.cod_actividad::text
                    AND t.cod_subactividad::text = e.cod_subactividad::text
                    AND t.cod_servicio::text     = e.cod_servicio::text
        """

        # Truncar partición del mes antes de cargar datos
        partition_name = f"dwsge.dwe_farm_{anio}_{mes_str}"
        try:
            cur_dst = conn_dst.cursor()
            cur_dst.execute(f"TRUNCATE TABLE {partition_name};")
            conn_dst.commit()
            cur_dst.close()
            print(f"Partición truncada correctamente: {partition_name}")
        except Exception as e:
            conn_dst.rollback()
            print(f"Error truncando partición {partition_name}: {e}")
            continue

        # Leer en bloques con cursor del lado del servidor (evita cargar todo el mes en memoria)
        cur_src = conn_src.cursor(name=f"cur_farm_{anio}_{mes_str}")
        cur_src.itersize = chunk_size
        cur_src.execute(query)

        total = 0
        bloque = 0
        while True:
            rows = cur_src.fetchmany(chunk_size)
            if not rows:
                break
            bloque += 1
            df = pd.DataFrame(rows, columns=[d[0].lower() for d in cur_src.description])

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

            total += len(df)
            print(f"  Bloque {bloque} cargado: {len(df)} filas (acumulado {total})")

        cur_src.close()
        conn_src.commit()

        if total == 0:
            print(f"No se encontraron datos para el mes {mes_str}")
        else:
            cur_dst = conn_dst.cursor()
            conn_dst.commit()
            cur_dst.close()
            print(f"Carga con COPY completada mes {mes_str}: filas totales cargadas {total}")

    except Exception as e:
        conn_src.rollback()
        conn_dst.rollback()
        print(f"Error en mes {mes_str}: {e}")

conn_src.close()
conn_dst.close()

end_time = datetime.now()
print(f"Proceso finalizado en {end_time - start_time}")
