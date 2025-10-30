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

table_name = "dssge.dwe_emergencia_estancia_homologacion"


for mes in range(1,10):
    mes_str = f"{mes:02d}"
    try:
        cur_dst = conn_dst.cursor()
        partition_name = f"dssge.dwe_emergencia_estancia_homologacion_{anio}_{mes_str}"
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
                WITH mt_base AS (
                SELECT
                    mt.*,
                    -- horas totales como número
                    (EXTRACT(EPOCH FROM (
                        (TO_DATE(mt.admemealtadmfec, 'DD-MM-YYYY') + COALESCE(mt.admemealtadmhor, '00:00')::time)
                    - (TO_DATE(mt.ADMEMEADMFEC,  'DD-MM-YYYY') + COALESCE(mt.ADMEMEADMHOR,  '00:00')::time)
                    )) / 3600.0) AS horas_totales
                FROM dssge.sgss_mtade10_{anio}_{mes_str} mt
                WHERE mt.ADMEMEAREHOSCOD = '02'  -- filtramos pronto para reducir filas
                ),

                -- últimas filas por (origen, centro, acto_med)
                diag_latest AS (
                SELECT
                    d.ateemeoricenasicod,
                    d.ateemecenasicod,
                    d.ateemeactmednum,
                    d.diagcod,
                    ROW_NUMBER() OVER (
                    PARTITION BY d.ateemeoricenasicod, d.ateemecenasicod, d.ateemeactmednum
                    ORDER BY c.ateemesecnum DESC
                    ) AS rn
                FROM dssge.sgss_mtdae10_{anio}_{mes_str} d
                JOIN dssge.sgss_mtaem10_{anio}_{mes_str} c
                    ON c.ateemeoricenasicod = d.ateemeoricenasicod
                AND c.ateemecenasicod   = d.ateemecenasicod
                AND c.ateemeactmednum   = d.ateemeactmednum
                AND c.ateemesecnum      = d.ateemesecnum
                ),

                diag_latest_sel AS (
                -- quedarnos solo con la fila más reciente por grupo (rn = 1)
                SELECT ateemeoricenasicod, ateemecenasicod, ateemeactmednum, diagcod
                FROM diag_latest
                WHERE rn = 1
                ),

                -- CTE para filtrar sólo mt que tengan algún diagnóstico NO NULO
                diag_any AS (
                SELECT DISTINCT ateemeoricenasicod, ateemecenasicod, ateemeactmednum
                FROM dssge.sgss_mtdae10_{anio}_{mes_str} d
                WHERE d.diagcod IS NOT NULL
                )

                SELECT
                mt.admemeoricenasicod AS cod_oricentro,
                mt.admemecenasicod AS cod_centro,
                mt.anio,
                mt.periodo,
                h.cod_estandar,
                mt.actmedtipopacicod as cod_tipo_paciente,
                mt.actmednum AS acto_med,
                mt.actmedarehoscod AS cod_area,
                mt.actmedservhoscod AS cod_servicio,
                mt.tipoparecod AS cod_tipo_parentesco,
                mt.ADMEMEADMFEC AS fecha_admision,
                mt.admemealtadmfec AS fecha_alta,
                mt.admemealtadmhor AS hora_alta_adm,
                mt.ADMEMEADMHOR AS hora_admision,

                -- formateo HH:MM a partir de horas_totales 
                    LPAD(TRUNC(mt.horas_totales)::text, 6, '0') || ':' ||
                    LPAD(ROUND((mt.horas_totales - TRUNC(mt.horas_totales)) * 60)::int::text, 2, '0') AS estancia_horas,

                CASE WHEN mt.horas_totales <= 24 THEN 2 ELSE 1 END AS rango_estancia,

                COALESCE(dl.diagcod, 'S/COD') AS cod_diag_emer

                FROM mt_base mt

                -- garantizar que exista al menos 1 diagnóstico NO NULO 
                INNER JOIN diag_any da
                ON da.ateemeoricenasicod = mt.ADMEMEORICENASICOD
                AND da.ateemecenasicod   = mt.ADMEMECENASICOD
                AND da.ateemeactmednum   = mt.ADMEMEACTMEDNUM

                -- left join para traer el diag más reciente 
                LEFT JOIN diag_latest_sel dl
                ON dl.ateemeoricenasicod = mt.ADMEMEORICENASICOD
                AND dl.ateemecenasicod   = mt.ADMEMECENASICOD
                AND dl.ateemeactmednum   = mt.ADMEMEACTMEDNUM

                LEFT JOIN dssge.dw_homologacion_enlaces_emergencia h
                ON h.cod_centro   = mt.admemecenasicod
                AND h.cod_topico   = mt.ADMEMETOPEMECOD
                AND h.cod_emergencia = mt.ADMEMEEMECOD

                WHERE
                h.cod_estandar IN ('01')
    
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