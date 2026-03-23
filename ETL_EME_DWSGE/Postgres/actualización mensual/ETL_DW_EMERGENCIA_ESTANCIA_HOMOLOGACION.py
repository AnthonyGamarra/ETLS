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

table_name = "dwsge.dwe_emergencia_estancia_homologacion"
time= datetime.now().strftime("'%Y-%m-%d %H:%M:%S'")

for mes in range(3,4):
    mes_str = f"{mes:02d}"
    print(f"\nProcesando mes: {anio}-{mes_str}")
    try:
        query = f"""
            SELECT
                t1.admemeoricenasicod                                               AS cod_oricentro,
                t1.admemecenasicod                                                  AS cod_centro,
                t1.anio,
                t1.periodo,
                h.cod_estandar,
                t1.actmedtipopacicod                                                AS cod_tipo_paciente,
                t1.actmedarehoscod                                                  AS cod_area,
                t1.actmedservhoscod                                                 AS cod_servicio,
                t1.tipoparecod                                                      AS cod_tipo_parentesco,
                t1.admemeactmednum                                                  AS acto_med,
                t1.admemeadmfec                                                     AS fecha_admision,
                t1.admemeadmhor                                                     AS hora_admision,
                t1.admemealtadmfec                                                  AS fecha_alta,
                t1.admemealtadmhor                                                  AS hora_alta_adm,
                dlast.diagcod                                                       AS cod_diag_emer,

                /* === Estancia en HORAS REALES (HHH:MM) === */
                (
                    floor(calc.segundos / 3600)::text
                    || ':' ||
                    lpad(floor(mod(calc.segundos / 60, 60))::text, 2, '0')
                ) AS estancia_horas,

                /* === RANGO DE ESTANCIA === */
                CASE
                    WHEN calc.segundos >= 24 * 3600 THEN 1  -- >= 24 horas
                    ELSE 2                                  -- < 24 horas
                END AS rango_estancia

            FROM dssge.sgss_mtade10_{anio}_{mes_str} t1

            LEFT JOIN dssge.dw_homologacion_enlaces_emergencia h
                ON h.cod_centro     = t1.admemecenasicod
            AND h.cod_topico     = t1.admemetopemecod
            AND h.cod_emergencia = t1.admemeemecod

            LEFT JOIN LATERAL (
                SELECT d.diagcod
                FROM dssge.sgss_mtdae10_{anio} d
                INNER JOIN dssge.sgss_mtaem10_{anio} c
                        ON c.ateemeoricenasicod = d.ateemeoricenasicod
                    AND c.ateemecenasicod    = d.ateemecenasicod
                    AND c.ateemeactmednum    = d.ateemeactmednum
                    AND c.ateemesecnum       = d.ateemesecnum
                WHERE d.ateemeoricenasicod = t1.admemeoricenasicod
                AND d.ateemecenasicod    = t1.admemecenasicod
                AND d.ateemeactmednum    = t1.admemeactmednum
                AND d.ateemediagord ='1'
                ORDER BY c.ateemesecnum DESC
                LIMIT 1
            ) dlast ON true

            /* === Cálculo único correcto de duración === */
            CROSS JOIN LATERAL (
                SELECT
                    extract(epoch FROM (
                        to_timestamp(t1.admemealtadmfec || ' ' || t1.admemealtadmhor, 'DD-MM-YYYY HH24:MI:SS')
                    - to_timestamp(t1.admemeadmfec     || ' ' || t1.admemeadmhor,     'DD-MM-YYYY HH24:MI:SS')
                    )) AS segundos
            ) calc

            WHERE
                t1.admemearehoscod = '02'
                AND COALESCE(dlast.diagcod, 'S/COD') <> 'S/COD'
                AND h.cod_estandar IN ('01');
        """
        actualizacion = f"""UPDATE dwsge.fecha_act
                            SET fecha_act = {time}
                            WHERE id=9"""
        df = pd.read_sql_query(query, conn_src)

        if df.empty:
            print(f"No se encontraron datos para el mes {mes_str}")
        else:
            try:
                cur_dst = conn_dst.cursor()
                partition_name = f"dwsge.dwe_emergencia_estancia_homologacion_{anio}_{mes_str}"
                cur_dst.execute(f"TRUNCATE TABLE {partition_name};")
                conn_dst.commit()
                cur_dst.close()
                print(f"Partición truncada correctamente: {partition_name}")
            except Exception as e:
                print(f"Error truncando partición {partition_name}: {e}")   
            # Truncar partición del mes antes de cargar datos

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