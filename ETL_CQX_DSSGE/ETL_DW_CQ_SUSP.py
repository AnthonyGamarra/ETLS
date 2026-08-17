import os
import oracledb
import pandas as pd
from io import StringIO
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# ==============================
# 1. Cargar variables desde .env
# ==============================
load_dotenv()

# Oracle
oracle_user = os.getenv("ORACLE_USER")
oracle_pass = os.getenv("ORACLE_PASS")
oracle_host = os.getenv("ORACLE_HOST")
oracle_port = os.getenv("ORACLE_PORT")
oracle_service = os.getenv("ORACLE_SERVICE")

# PostgreSQL
pg_user = os.getenv("PG_USER")
pg_pass = os.getenv("PG_PASS")
pg_host = os.getenv("PG_HOST")
pg_port = os.getenv("PG_PORT", "5433")
pg_db   = os.getenv("PG_DB")

# ==============================
# 2. Función para obtener rango mensual
# ==============================
def month_range(start_date, end_date):
    current = start_date
    limite = (end_date.replace(day=1) + relativedelta(months=1))
    while current < limite:
        start_mes = current
        end_mes = (current.replace(day=1) + relativedelta(months=1))
        yield start_mes, end_mes
        current = end_mes

# ==============================
# 3. Conexión Oracle
# ==============================
print(f"Conectando a Oracle en {oracle_host}:{oracle_port}/{oracle_service}...")
dsn = f"{oracle_host}:{oracle_port}/{oracle_service}"
conn_oracle = oracledb.connect(user=oracle_user, password=oracle_pass, dsn=dsn)
print("Conexión a Oracle establecida.")

# ==============================
# 4. Conexión PostgreSQL
# ==============================
print(f"Conectando a PostgreSQL en {pg_host}:{pg_port}, base de datos: {pg_db}...")
conn_pg = psycopg2.connect(
    host=pg_host,
    database=pg_db,
    user=pg_user,
    password=pg_pass,
    port=pg_port
)
conn_pg.autocommit = True
cursor_pg = conn_pg.cursor()
print("Conexión a PostgreSQL establecida.")

# ==============================
# 5. Parámetros de fechas
# ==============================
start_date = datetime(2026, 1, 1)
end_date = datetime(2026, 8, 31)

# ==============================
# 6. Ciclo para extraer y copiar mes a mes
# ==============================
for start_mes, end_mes in month_range(start_date, end_date):
    print(f"\n--- Procesando mes: {start_mes.strftime('%Y-%m')} ---")
    anio = start_mes.strftime('%Y')
    mes  = start_mes.strftime('%m')  # <-- Asegura formato 01,02,03...
    start_mes_str = start_mes.strftime('%d-%m-%Y')
    end_mes_str = end_mes.strftime('%d-%m-%Y')
    tabla_destino = f"dssge.dw_cq_sus_{anio}_{mes}"
    

    query = f"""
            SELECT
            to_char(T.Solopeprofec, 'yyyymm')                    		 								as PERIODO,
            to_char(T.Solopeprofec, 'yyyy')                             								as ANIO,
            T.SOLOPECENASICOD                         AS COD_CENTRO,
            T.SOLOPESERVHOSCOD                        AS COD_SERV_PROCED,
            E.PERAUTCOD                               AS AUTOGENERADO,
            E.PERDOCIDENNUM                           AS DOC_PACIENTE,
            D.ACTMEDEDADATEN                          AS EDAD,
            M.TIPSEGCOD                               AS COD_TIPO_SEGURO,
            N.TIPOPACICOD                             AS COD_TIPO_PACIENTE,
            T.SOLOPENUM                               AS NUM_SOLICITUD,
            TO_CHAR(T.SOLOPEFEC, 'dd/mm/yyyy')        AS FECHA_SOLICITUD,
            TO_CHAR(T.SOLOPESOLFEC, 'dd/mm/yyyy')     AS FECHA_PROGRAMACION,
            O.SOLOPEDIAGCOD                           AS COD_DIAG,
            G.GRDCMCCOD                               AS COD_COMPLEJIDAD,
            T.MOTSOPCOD                                                AS COD_MOT_SUSPENSION,
            T.SOLOPEUSUCRECOD                                          AS USUREG,
            TO_CHAR(T.SOLOPECREFEC, 'dd/mm/yyyy')                      AS FECREG,
            T.SOLOPEUSUMODCOD                                          AS USUMODIF,
            TO_CHAR(T.SOLOPEMODFEC, 'dd/mm/yyyy')                      AS FECMODIF,
            T.SOLOPECENQUICOD                                          AS COD_QUIROF,
            T.SOLOPESOLSERVHOSCOD                                      AS COD_SERV_SOLICITADA,
            C.SOLOPECPSCOD                                             AS COD_CPS
            FROM SGSS.QTSOP10 T
            LEFT OUTER JOIN SGSS.QTSOD10 O ON T.SOLOPEORICENASICOD = O.SOLOPEORICENASICOD
                                    AND T.SOLOPECENASICOD = O.SOLOPECENASICOD
                                    AND T.SOLOPENUM = O.SOLOPENUM
            LEFT OUTER JOIN SGSS.QTSOO10 C ON T.SOLOPEORICENASICOD = C.SOLOPEORICENASICOD
                                    AND T.SOLOPECENASICOD = C.SOLOPECENASICOD
                                    AND T.SOLOPENUM = C.SOLOPENUM
            LEFT OUTER JOIN SGSS.CMAME10 D ON T.SOLOPEORICENASICOD = D.ORICENASICOD
                                    AND T.SOLOPECENASICOD = D.CENASICOD
                                    AND T.SOLOPEACTMEDNUM = D.ACTMEDNUM
            LEFT OUTER JOIN SGSS.CMPER10 E ON D.ACTMEDPACSECNUM = E.PERSECNUM
            LEFT OUTER JOIN SGSS.CBTPC10 N ON D.ACTMEDTIPOPACICOD = N.TIPOPACICOD
            LEFT OUTER JOIN SGSS.CMTSE10 M ON D.ACTMEDTIPSEGCOD = M.TIPSEGCOD
            LEFT OUTER JOIN SGSS.CMCPP10 F ON C.SOLOPECPSCOD = F.CPSCOD
            LEFT OUTER JOIN SGSS.QBGCC10 G ON F.GRDCMCCOD = G.GRDCMCCOD
            WHERE  
            T.Solopeprofec >= TO_DATE('{start_mes_str}', 'DD-MM-YYYY')
            AND T.Solopeprofec < TO_DATE('{end_mes_str}', 'DD-MM-YYYY')
            AND T.ESTSOPCOD = '3'
            

    """

    print(f"Ejecutando query para mes {start_mes.strftime('%Y-%m')} en Oracle...")
    df = pd.read_sql(query, conn_oracle)
    print(f"Datos extraídos: {len(df)} filas.")

    if df.empty:
        print("No hay datos para este mes.")
        continue

    df.columns = df.columns.str.lower()

    # ==============================
    # TRUNCAR PARTICIÓN DESTINO
    # ==============================
    print(f"Truncando partición destino: {tabla_destino}...")
    try:
        cursor_pg.execute(f"TRUNCATE TABLE {tabla_destino};")
        print(f"Tabla {tabla_destino} truncada correctamente.")
    except Exception as e:
        print(f"⚠️ Error al truncar {tabla_destino}: {e}")
        continue  # Saltar este mes si no existe la partición

    # Guardamos el DataFrame en un buffer CSV en memoria
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)

    # Usamos COPY para cargar datos a PostgreSQL
    print(f"Cargando datos a PostgreSQL para mes {start_mes.strftime('%Y-%m')}...")
    try:
        cursor_pg.copy_expert(
            sql=f"COPY {tabla_destino} ({', '.join(df.columns)}) FROM STDIN WITH CSV",
            file=csv_buffer
        )
        print(f"Mes {start_mes.strftime('%Y-%m')} cargado correctamente.")
    except Exception as e:
        print(f"Error al cargar mes {start_mes.strftime('%Y-%m')}: {e}")

# ==============================
# 7. Cerramos conexiones
# ==============================
print("\nCerrando conexiones a bases de datos...")
cursor_pg.close()
conn_pg.close()
conn_oracle.close()
print("Conexiones cerradas. Proceso finalizado.")




