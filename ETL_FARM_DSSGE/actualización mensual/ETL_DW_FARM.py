import os
import oracledb
import pandas as pd
from io import StringIO
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta

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
    while current <= end_date:
        next_month = (current.replace(day=1) + timedelta(days=32)).replace(day=1)
        yield current, min(next_month - timedelta(days=1), end_date)
        current = next_month

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
start_date = datetime(2019, 1, 1)
end_date = datetime(2024, 12, 31)

# ==============================
# 6. Ciclo para extraer y copiar mes a mes en bloques semanales
# ==============================
for start_mes, end_mes in month_range(start_date, end_date):
    print(f"\n--- Procesando mes: {start_mes.strftime('%Y-%m')} ---")
    anio = start_mes.strftime('%Y')
    mes  = start_mes.strftime('%m')  # <-- Asegura formato 01,02,03...
    tabla_destino = f"dssge.dw_farm_{anio}_{mes}"

    print(f"Truncando partición destino: {tabla_destino}...")
    try:
        cursor_pg.execute(f"TRUNCATE TABLE {tabla_destino};")
        print(f"Tabla {tabla_destino} truncada correctamente.")
    except Exception as e:
        print(f"⚠️ Error al truncar {tabla_destino}: {e}")
        continue

    week_start = start_mes
    week_num = 1
    while week_start <= end_mes:
        week_end = min(week_start + timedelta(days=6), end_mes)
        print(f"\nProcesando semana {week_num}: {week_start.strftime('%Y-%m-%d')} a {week_end.strftime('%Y-%m-%d')}...")

        query = f"""
                SELECT
                TO_CHAR(t.movmatdocfec,'YYYY')                               AS anio,
                TO_CHAR(t.movmatdocfec,'YYYYMM')                             AS periodo,
                t.oricenasicod                                               AS  COD_ORICENTRO,
                t.cenasicod                                                  AS  COD_CENTRO,
                T.MOVMATAREHOSCOD                                            AS COD_AREA,
                T.MOVMATSERVHOSCOD                                           AS COD_SERVICIO,
                T.MOVMATACTCOD                                               AS COD_ACTIVIDAD,
                T.MOVMATACTESPCOD                                            AS COD_SUBACTIVIDAD,
                fc.actmednum                                                 AS NUM_ACTOMED,
                H.perasisdocidennum                                          AS DNI_PROFESIONAL,
                H.perasisprocolcod                                           AS CMP,
                to_char(fc.solmatdocfec,'dd/mm/yyyy')                        AS FECHA_SOLICITUD,
                fc.solmatdocnum                                              AS NUM_RECETA,
                t.matcod                                                     AS COD_MEDICAMENTO,
                fd.solmadcan                                                 AS CANT_SOLICITUD,
                --fd.solmadate                                               AS CANT_ATENDIDA,
                decode(t.movmattiptrncod,
                    '2', (t.movmatcan),
                    '1', ((t.movmatcan) * -1))                               AS CANT_ATENDIDA,
                k.uniatecod                                                  AS UNIDAD,
                fd.solmaddiascan                                             AS DURACION_MED,
                fd.solmaddiagcod                                             AS COD_DX,
                c.perautcod                                                  AS AUTOGENERADO,
                c.perdocidennum                                              AS DNI,
                decode(c.persexocod,'1','M','0','F','')                      AS SEXO,
                (FLOOR(MONTHS_BETWEEN(t.movmatdocfec,c.pernacfec) / 12))     AS EDAD_ANIO,
                (FLOOR(MOD(MONTHS_BETWEEN(t.movmatdocfec,c.pernacfec), 12))) AS MESES,
                g.almcod                                                     AS COD_FARMACIA,
                t.movmatusucrecod                                            AS CODUSU_DESPACHO,
                pe.grupocupcod                                               AS COD_GRPOCUPACIONAL_DESP,
                to_char(t.movmatcrefec,'dd/mm/yyyy')                         AS FECHA_DESPACHO,
                to_char(t.movmatcrefec,'hh24:mi')                            AS HORA_DESPACHO,
                fc.solmatsicrecenum                                          AS NUMRECETMANUAL,
                j.almmatprepro                                               AS PRECIO,
                c.pertipsegcod                                               AS COD_TIPO_SEGURO,
                c.pertipoparecod                                             AS COD_TIPO_PARENTESCO,
                am.actmedtipopacicod                                         AS COD_TIPO_PACIENTE,
                k1.TIPMOVCOD                                                 AS TIPO_MOVIMIENTO
                from sgss.ftmma10 t
                left outer join sgss.ftsmd10 fd  on fd.oricenasicod = t.oricenasicod
                                        and fd.cenasicod    = t.cenasicod
                                        and fd.solalmcod    = t.movmatsolalmcod
                                        and fd.solmatdocnum = t.movmatsolmatdocnum
                                        AND fd.matcod       = t.matcod
                left outer join sgss.ftsma10 fc  on fc.oricenasicod = fd.oricenasicod
                                        and fc.cenasicod    = fd.cenasicod
                                        and fc.solalmcod    = fc.solalmcod
                                        and fc.solmatdocnum = fd.solmatdocnum
                LEFT OUTER JOIN sgss.cmame10 am ON am.oricenasicod  = fc.oricenasicod
                                        AND am.cenasicod    = fc.cenasicod
                                        AND am.actmednum    = fc.actmednum
                left outer join sgss.fmalm10 g on  t.oricenasicod   = g.oricenasicod
                                        and t.cenasicod      = g.cenasicod
                                        and t.tipalmcod      = g.tipalmcod
                                        and t.almcod         = g.almcod
                left outer join sgss.cmprs10 h on fc.solmattipdocidenpercod   = h.tipdocidenpercod
                                        and fc.solmatperasisdocidennum = h.perasisdocidennum
                left outer join sgss.fmmat10 k on  t.matcod           = k.matcod
                left outer join sgss.cmper10 c on  fc.solmatpacsecnum = c.persecnum
                left outer join sgss.fmama10 j on  j.oricenasicod = t.oricenasicod
                                        and j.cenasicod    = t.cenasicod
                                        and j.tipalmcod    = t.tipalmcod
                                        and j.almcod       = t.almcod
                                        and j.matcod       = t.matcod
                left outer join sgss.fmtmo10 k1 on t.tipmovcod    = k1.tipmovcod
                LEFT OUTER JOIN sgss.cmprs10 pe ON pe.perasisdocidennum = t.movmatusucrecod
                WHERE
                fd.solalmcod          = '02' -- recetas (tabla fmtsa10)
                and t.matcod like '01%' -- medicamentos
                and fc.solmatestregcod   = '1'
                AND fd.estatematcod IN ('1','2') ---  atendidos
                AND t.oricenasicod  = '1'
                AND t.movmatdocfec >= TO_DATE('{week_start.strftime('%d-%m-%Y')}', 'DD-MM-YYYY')
                AND t.movmatdocfec <  TO_DATE('{(week_end + timedelta(days=1)).strftime('%d-%m-%Y')}', 'DD-MM-YYYY')
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

# 7. Cerramos conexiones
# ==============================
print("\nCerrando conexiones a bases de datos...")
cursor_pg.close()
conn_pg.close()
conn_oracle.close()
print("Conexiones cerradas. Proceso finalizado.")
