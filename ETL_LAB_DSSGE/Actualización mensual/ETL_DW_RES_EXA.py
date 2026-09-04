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
start_date = datetime(2026, 3, 1)
end_date = datetime(2026, 3, 31)

# ==============================
# 6. Ciclo para extraer y copiar mes a mes en bloques semanales
# ==============================
for start_mes, end_mes in month_range(start_date, end_date):
    print(f"\n--- Procesando mes: {start_mes.strftime('%Y-%m')} ---")
    anio = start_mes.strftime('%Y')
    mes  = start_mes.strftime('%m')  # <-- Asegura formato 01,02,03...
    tabla_destino = f"dssge.dw_lab_{anio}_{mes}"
    fin_mes_incl = end_mes - timedelta(days=1)  # end_mes es exclusivo (1er dia del mes siguiente)

    print(f"Truncando partición destino: {tabla_destino}...")
    try:
        cursor_pg.execute(f"TRUNCATE TABLE {tabla_destino};")
        print(f"Tabla {tabla_destino} truncada correctamente.")
    except Exception as e:
        print(f"⚠️ Error al truncar {tabla_destino}: {e}")
        continue  # Saltar este mes si no existe la partición

    week_start = start_mes
    week_num = 1
    while week_start <= fin_mes_incl:
        week_end = min(week_start + timedelta(days=6), fin_mes_incl)
        print(f"\nProcesando semana {week_num}: {week_start.strftime('%Y-%m-%d')} a {week_end.strftime('%Y-%m-%d')}...")

        query = f"""
                SELECT distinct
                to_char(x.resexafec, 'yyyy')                               AS ANIO,
                to_char(x.resexafec, 'yyyymm')                               AS PERIODO,
                x.RESEXAORICENASICOD                                           AS COD_ORICENTRO,
                x.resexacenasicod                                            AS COD_CENTRO,
                n1.AREAEXACOD                                                AS COD_AREALAB,
                f.AREHOSCOD                                                  AS COD_AREA,
                h.servhoscod                                               AS COD_SERVICIO,
                i.actcod                                                     AS COD_ACTIVIDAD,
                j.actespcod                                                  AS COD_SUBACTIVIDAD,
                p.perapepatdes||' '||p.perapematdes|| ' ' ||
                p.perprinomdes || ' ' ||p.persegnomdes                       AS PACIENTE,
                p.pertipdocidencod                                           AS TIPO_DOC_PACIENTE,
                b.SOLEXAACTMEDORINUM                                           AS ACTO_MED,
                t.actmedtipopacicod                                           AS COD_TIPO_PACIENTE,
                p.perdocidennum                                              AS DOC_PACIENTE,
                decode(p.persexocod, '1', 'M', '0', 'F', '')                 AS SEXO,
                (FLOOR(MONTHS_BETWEEN(x.resexafec, p.pernacfec) / 12))       AS ANIO_EDAD,
                (FLOOR(MOD(MONTHS_BETWEEN(x.resexafec, p.pernacfec), 12)))   AS MESES,
                p.percenasiadscod                                            AS CAS_ADSCRIPCION,
                x.resexafec                                                     AS FECHA_EXAMEN,
                y.tipexacod                                                   AS COD_TIPOEXAMEN,
                x.resexacpscod                                               AS COD_CPMS,
                replace(replace(trim( to_char(
                substr(x.resexainf,0,100))),CHR(10), ''), CHR(13), '')      AS INFORME_RESULTADO,
                v.resexvplldetord                                           AS ORDEN_PLANTILLA,
                v.resexdexaprudes                                           AS DESC_PLANTILLA,
                v.resexdexades                                              AS VALOR_RESULTADO,
                v.resexdexaund                                              AS UNIDADVALOR,
                v.resexdexanorval                                           AS VALORESMASCULINO,
                v.resexdfemexanorval                                        AS VALORESFEMENINO,
                v.resexdexaotrnorval                                        AS VALORES_OTROS,
                v.resexdexaobs                                              AS OBS_RESULTADO
                from SGSS.etrea10 x
                left outer join SGSS.etsea10 b ON b.solexaoricenasicod = x.resexaoricenasicod
                                        AND b.solexacenasicod = x.resexacenasicod
                                        AND b.solexatipexacod = x.resexatipexacod
                                        AND b.solexanum       = x.resexasolexanum
                LEFT OUTER JOIN SGSS.ebtea10 y ON y.tipexacod = x.resexatipexacod
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
                WHERE x.resexafec        >= TO_DATE('{week_start.strftime('%d-%m-%Y')}','DD-MM-YYYY')
                AND x.resexafec        <  TO_DATE('{(week_end + timedelta(days=1)).strftime('%d-%m-%Y')}','DD-MM-YYYY')
        """

        print(f"Ejecutando query para semana {week_num} del mes {start_mes.strftime('%Y-%m')} en Oracle...")
        df = pd.read_sql(query, conn_oracle)
        print(f"Datos extraídos semana {week_num}: {len(df)} filas.")

        if df.empty:
            print(f"No hay datos para la semana {week_num}.")
            week_start = week_end + timedelta(days=1)
            week_num += 1
            continue

        df.columns = df.columns.str.lower()

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)

        print(f"Cargando datos a PostgreSQL para semana {week_num} del mes {start_mes.strftime('%Y-%m')}...")
        try:
            cursor_pg.copy_expert(
                sql=f"COPY {tabla_destino} ({', '.join(df.columns)}) FROM STDIN WITH CSV",
                file=csv_buffer
            )
            print(f"Semana {week_num} cargada correctamente.")
        except Exception as e:
            print(f"Error al cargar semana {week_num}: {e}")

        week_start = week_end + timedelta(days=1)
        week_num += 1

# ==============================
# 7. Cerramos conexiones
# ==============================
print("\nCerrando conexiones a bases de datos...")
cursor_pg.close()
conn_pg.close()
conn_oracle.close()
print("Conexiones cerradas. Proceso finalizado.")