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

table_name = "DWH_SGE.dwe_emergencia_estancia_homologacion"


for mes in range(1,12):
    mes_str = f"{mes:02d}"

    try:
        query = f"""
                WITH mt_base AS (
                    SELECT
                        mt.*,
                        -- horas totales como número truncado a 2 decimales
                        TRUNC(
                            (EXTRACT(EPOCH FROM (
                                (TO_DATE(mt.admemealtadmfec, 'DD-MM-YYYY') + COALESCE(mt.admemealtadmhor, '00:00')::time)
                            - (TO_DATE(mt.ADMEMEADMFEC,  'DD-MM-YYYY') + COALESCE(mt.ADMEMEADMHOR,  '00:00')::time)
                            )) / 3600.0),
                            2
                        ) AS horas_totales
                    FROM dssge.sgss_mtade10_{anio}_{mes_str} mt
                    WHERE mt.ADMEMEAREHOSCOD = '02'
                ),

                -- Diagnóstico más reciente por acto médico
                diag_latest_sel AS (
                    SELECT DISTINCT ON (d.ateemeoricenasicod, d.ateemecenasicod, d.ateemeactmednum)
                        d.ateemeoricenasicod,
                        d.ateemecenasicod,
                        d.ateemeactmednum,
                        d.diagcod
                    FROM dssge.sgss_mtdae10_{anio}_{mes_str} d
                    JOIN dssge.sgss_mtaem10_{anio}_{mes_str} c
                    ON c.ateemeoricenasicod = d.ateemeoricenasicod
                    AND c.ateemecenasicod   = d.ateemecenasicod
                    AND c.ateemeactmednum   = d.ateemeactmednum
                    AND c.ateemesecnum      = d.ateemesecnum
                    WHERE d.diagcod IS NOT NULL
                    ORDER BY d.ateemeoricenasicod, d.ateemecenasicod, d.ateemeactmednum, c.ateemesecnum DESC
                ),

                -- Atenciones con algún diagnóstico no nulo
                diag_any AS (
                    SELECT DISTINCT 
                        d.ateemeoricenasicod,
                        d.ateemecenasicod,
                        d.ateemeactmednum
                    FROM dssge.sgss_mtdae10_{anio}_{mes_str} d
                    WHERE d.diagcod IS NOT NULL
                )

                SELECT
                    mt.admemeoricenasicod AS cod_oricentro,
                    mt.admemecenasicod    AS cod_centro,
                    mt.anio,
                    mt.periodo,
                    h.cod_estandar,
                    mt.actmedtipopacicod  AS cod_tipo_paciente,
                    mt.actmednum          AS acto_med,
                    mt.actmedarehoscod    AS cod_area,
                    mt.actmedservhoscod   AS cod_servicio,
                    mt.tipoparecod        AS cod_tipo_parentesco,
                    mt.ADMEMEADMFEC       AS fecha_admision,
                    mt.admemealtadmfec    AS fecha_alta,
                    mt.admemealtadmhor    AS hora_alta_adm,
                    mt.ADMEMEADMHOR       AS hora_admision,

                    -- formateo HH:MM desde horas_totales 
                    LPAD(TRUNC(mt.horas_totales)::text, 6, '0') || ':' ||
                    LPAD(ROUND((mt.horas_totales - TRUNC(mt.horas_totales)) * 60)::int::text, 2, '0') AS estancia_horas,

                    CASE 
                        WHEN mt.horas_totales <= 24 THEN 2 
                        ELSE 1 
                    END AS rango_estancia

                FROM mt_base mt

                -- garantizar que exista al menos 1 diagnóstico NO NULO 
                INNER JOIN diag_any da
                ON da.ateemeoricenasicod = mt.ADMEMEORICENASICOD
                AND da.ateemecenasicod   = mt.ADMEMECENASICOD
                AND da.ateemeactmednum   = mt.ADMEMEACTMEDNUM

                -- left join para traer el diagnóstico más reciente 
                LEFT JOIN diag_latest_sel dl
                ON dl.ateemeoricenasicod = mt.ADMEMEORICENASICOD
                AND dl.ateemecenasicod   = mt.ADMEMECENASICOD
                AND dl.ateemeactmednum   = mt.ADMEMEACTMEDNUM

                -- tabla de homologación
                LEFT JOIN dssge.dw_homologacion_enlaces_emergencia h
                ON h.cod_centro     = mt.admemecenasicod
                AND h.cod_topico     = mt.ADMEMETOPEMECOD
                AND h.cod_emergencia = mt.ADMEMEEMECOD

                WHERE h.cod_estandar IN ('01');
    
        """

        df = pd.read_sql_query(query, conn_src)

        # -------------------------------------------
        # Truncar partición Oracle
        # -------------------------------------------
        try:
            tabla = "DWH_SGE.dwe_emergencia_estancia_homologacion"
            partition_name = f"P{anio}_{mes_str}"

            sql_truncate = f"ALTER TABLE {tabla} TRUNCATE SUBPARTITION {partition_name}"

            with conn_dst.cursor() as cur:
                cur.execute(sql_truncate)
                conn_dst.commit()

            print(f"Partición {partition_name} truncada correctamente.")
        except Exception as e:
            print(f"Advertencia: error truncando partición {partition_name}: {e}")


        if df.empty:
            print(f"No se encontraron datos para el mes {mes_str}")
        else:
            df.columns = df.columns.str.lower()

        # ======================================================
        # INSERTAR EN ORACLE USANDO executemany
        # ======================================================
                # Cargar datos
        table_name = "DWH_SGE.dwe_emergencia_estancia_homologacion"
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