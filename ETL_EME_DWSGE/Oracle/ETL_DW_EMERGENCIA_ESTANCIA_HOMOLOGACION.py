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

anio = datetime.now().year -1
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


for mes in range(1,13):
    mes_str = f"{mes:02d}"

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