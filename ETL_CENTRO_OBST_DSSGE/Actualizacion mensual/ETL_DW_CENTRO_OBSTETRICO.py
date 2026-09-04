import os
import oracledb
import polars as pl
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

start_time = datetime.now()

today = datetime.today()
current_year = today.year
current_month = today.month

if current_month == 1:
    year_target = current_year - 1
    month_target = 12
else:
    year_target = current_year
    month_target = current_month - 1

data_found = False

# ==============================
# 5. Parámetros de fechas
# ==============================
start_date = datetime(2026, 1, 1)
end_date = datetime(2026, 8, 31)

# ==============================
# 6. Ciclo para extraer y copiar mes a mes
# ==============================
for start_mes, end_mes in month_range(start_date, end_date):
    mes_start_time = datetime.now()
    print(f"\n--- Procesando mes: {start_mes.strftime('%Y-%m')} ---")
    print(f"🕒 Inicio del mes: {mes_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    anio = start_mes.strftime('%Y')
    mes  = start_mes.strftime('%m')  # <-- Asegura formato 01,02,03...
    tabla_destino = f"dssge.dwe_centro_obstetrico_{anio}_{mes}"
    
    query = f"""
            SELECT
                t.ATENGENORICENASICOD 															 AS COD_ORICENTRO,
                t.atengencenasicod   															 AS COD_CENTRO,
                to_char(atengenfec, 'yyyy')     													 AS ANIO,
                to_char(atengenfec, 'yyyymm')   													 AS PERIODO,	
                to_char(t.atengenfec,'dd/mm/yyyy')                                               AS FECHA_ATENCION,
                to_char(t.atengenhor,'hh24:mi')                                                  AS HORATENCION,
                t.atengenactmednum                                                               AS ACTO_MED,
                pa.pertipdocidencod                                                               AS COD_TIPDOC_PACIENTE,
                pa.perdocidennum                                                                  as DOC_PACIENTE, 
                am.actmedservhoscod                                       						 AS COD_SERVICIO,
                ee.estenfcod                                                                     AS COD_EST_ENFER,
                (FLOOR(MONTHS_BETWEEN(t.atengenfec, pa.pernacfec) / 12))                         AS ANNOS,
                decode(pa.persexocod, '1', 'M', '0', 'F', '')                                    AS SEXO,
                (select c.pachisclinum from SGSS.cmpac10 c where c.oricenasicod = am.oricenasicod and
                c.cenasicod = am.cenasicod and c.pacsecnum = am.actmedpacsecnum)                  AS H_C,
                am.actmedtipsegcod        														  AS COD_TIP_SEGURO,
                pa.pertipoparecod 																	  AS COD_TIPO_PARENTESCO,
                am.actmedtipopacicod																	  AS COD_TIPO_PACIENTE,
                pe.grupocupcod																	  AS COD_GRUPO_CUPACIONAL,
                pe.perasisprocolcod                                                               AS CPMS,
                pe.perasisdocidennum                                                              AS DOC_MEDICOL,
                dg.diagcod                                                                        AS COD_DIAG,
                pe.condtrabcod                          										  AS COD_COND_TRAB,
                pa.percenasiadscod                                                                           as CAS_ADSCRIPCION
            from SGSS.HTAGE10 t
            LEFT OUTER JOIN SGSS.hthos10 hs ON hs.hosporicenasicod = t.atengenoricenasicod
                                    AND hs.hospcenasicod    = t.atengencenasicod
                                    AND hs.hospactmednum    = t.atengenactmednum
            LEFT OUTER JOIN SGSS.hthod10 hd ON hd.hosporicenasicod = hs.hosporicenasicod
                                    AND hd.hospcenasicod    = hs.hospcenasicod
                                    AND hd.hospactmednum    = hs.hospactmednum
                                    AND hd.hosdnumsec       = hs.hospnumsec
            LEFT OUTER JOIN SGSS.hmese10 ee ON ee.oricenasicod     = hd.hosporicenasicod
                                    AND ee.cenasicod        = hd.hospcenasicod
                                    AND ee.arehoscod        = hd.hosdarehosintcod
                                    AND ee.servhoscod       = hd.hosdserhosintcod
                                    AND ee.estenfcod        = hd.hosdestenfcod                                                   
            LEFT OUTER JOIN SGSS.cmame10 am ON am.oricenasicod = t.atengenoricenasicod
                                    AND am.cenasicod   = t.atengencenasicod
                                    AND am.actmednum   = t.atengenactmednum
            LEFT OUTER JOIN SGSS.cmper10 pa ON pa.persecnum    = am.actmedpacsecnum                           
            LEFT OUTER JOIN SGSS.cmprs10 pe ON pe.tipdocidenpercod = t.atengentipdocidenpercod
                                    AND pe.perasisdocidennum = t.atengenperasisdocidennum
            LEFT OUTER JOIN SGSS.htdag10 dg ON dg.atengenoricenasicod = t.atengenoricenasicod
                                        AND dg.atengencenasicod    = t.atengencenasicod
                                        AND dg.atengenactmednum    = t.atengenactmednum
                                        AND dg.atengennumsec       = t.atengennumsec                             
            WHERE
            t.atengenfec >= to_date('{start_mes.strftime('%d-%m-%Y')}','DD-MM-YYYY')
            and t.atengenfec < TO_DATE('{(end_mes + timedelta(days=1)).strftime('%d-%m-%Y')}', 'DD-MM-YYYY')
            AND t.atengenarehoscod = '06'
    """

    print(f"Ejecutando query para mes {start_mes.strftime('%Y-%m')} en Oracle...")
    extraccion_start = datetime.now()
    df = pl.read_database(query=query, connection=conn_oracle, infer_schema_length=None)
    extraccion_time = datetime.now() - extraccion_start
    print(f"Datos extraídos: {len(df)} filas. (tiempo de extracción: {extraccion_time})")

    if df.is_empty():
        print("No hay datos para este mes.")
        continue

    df.columns = [c.lower() for c in df.columns]

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
    df.write_csv(
        csv_buffer,
        include_header=False,
        datetime_format="%Y-%m-%d %H:%M:%S",
        date_format="%Y-%m-%d",
    )
    csv_buffer.seek(0)

    # Usamos COPY para cargar datos a PostgreSQL
    print(f"Cargando datos a PostgreSQL para mes {start_mes.strftime('%Y-%m')}...")
    carga_start = datetime.now()
    try:
        cursor_pg.copy_expert(
            sql=f"COPY {tabla_destino} ({', '.join(df.columns)}) FROM STDIN WITH CSV",
            file=csv_buffer
        )
        carga_time = datetime.now() - carga_start
        print(f"Mes {start_mes.strftime('%Y-%m')} cargado correctamente. (tiempo de carga: {carga_time})")
    except Exception as e:
        print(f"Error al cargar mes {start_mes.strftime('%Y-%m')}: {e}")

    mes_end_time = datetime.now()
    print(f"⏱️ Mes {start_mes.strftime('%Y-%m')} procesado en {mes_end_time - mes_start_time}")

# ==============================
# 7. Cerramos conexiones
# ==============================
print("\nCerrando conexiones a bases de datos...")
cursor_pg.close()
conn_pg.close()
conn_oracle.close()
print("Conexiones cerradas. Proceso finalizado.")

end_time = datetime.now()
print(f"\n🕒 Fin del ETL: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"⏱️ Tiempo total de procesamiento: {end_time - start_time}")




