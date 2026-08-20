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
    tabla_destino = f"dssge.dw_lab_{anio}_{mes}"
    

    query = f"""
            SELECT distinct
            to_char(x.resexafec, 'yyyy')                               AS ANIO,
            to_char(x.resexafec, 'yyyymm')                               AS PERIODO,
            x.RESEXAORICENASICOD                                           AS COD_ORICENTRO,
            x.resexacenasicod                                            AS cod_CENTRO,
            n1.AREAEXACOD                                                AS cod_AREALAB,
            f.AREHOSCOD                                                  AS cod_AREA,
            h.servhoscod                                               AS COD_SERVICIO,
            i.actcod                                                     AS COD_ACTIVIDAD,
            j.actespcod                                                  AS COD_SUBACTIVIDAD,
            p.perdocidennum                                              AS COD_PACIENTE,
            decode(p.persexocod, '1', 'M', '0', 'F', '')                 AS SEXO,
            (FLOOR(MONTHS_BETWEEN(x.resexafec, p.pernacfec) / 12))       AS ANIO_EDAD,
            p.percenasiadscod                                            AS CAS_ADSCRIPCION,
            x.resexacpscod                                               AS EXAMEN,
            v.resexvplldetord                                           AS ORDEN_PLANTILLA,
            v.resexdexaund                                              AS UNIDADVALOR,
            v.resexdexanorval                                           AS VALORESMASCULINO,
            v.resexdfemexanorval                                        AS VALORESFEMENINO,                                       
            v.resexdexaotrnorval                                        AS VALORES_OTROS
            from SGSS.etrea10 x
            left outer join SGSS.etsea10 b ON b.solexaoricenasicod = x.resexaoricenasicod
                                    AND b.solexacenasicod = x.resexacenasicod
                                    AND b.solexatipexacod = x.resexatipexacod
                                    AND b.solexanum       = x.resexasolexanum
            LEFT OUTER JOIN SGSS.cmaho10 f ON f.arehoscod    = b.solexaarehoscod
            LEFT OUTER join SGSS.cmsho10 h ON h.servhoscod   = b.solexaservhoscod
            LEFT OUTER join SGSS.cmact10 i ON i.actcod = b.solexaactcod
            LEFT OUTER join SGSS.cmace10 j ON j.actcod = b.solexaactcod
                                        AND j.actespcod = b.solexaactespcod
            LEFT OUTER join SGSS.cmcpp10 l ON x.resexacpscod = l.cpscod
            LEFT OUTER JOIN SGSS.etred10 v ON v.resexaoricenasicod = x.resexaoricenasicod
                                    AND v.resexacenasicod   = x.resexacenasicod
                                    AND v.resexatipexacod   = x.resexatipexacod
                                    AND v.resexasolexanum   = x.resexasolexanum
                                    AND v.resexacpscod      = x.resexacpscod
            LEFT OUTER join SGSS.emaea10 n1 ON n1.tipexacod  = l.tipexacod
                                        AND n1.areaexacod = l.areaexacod
            LEFT OUTER join SGSS.cmame10 t ON t.oricenasicod = b.solexaoricenasioricod
                                    AND t.cenasicod    =  b.solexacenasioricod
                                    AND t.actmednum    = b.solexaactmedorinum                               
            LEFT OUTER join SGSS.cmper10 p ON p.persecnum = t.actmedpacsecnum
            WHERE x.resexafec        >= TO_DATE('{start_mes_str}','DD-MM-YYYY')
            AND x.resexafec        < TO_DATE('{end_mes_str}','DD-MM-YYYY')
            AND j.actespnom LIKE '%PREVENIR%'
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