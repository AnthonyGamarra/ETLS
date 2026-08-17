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
start_date = datetime(2020, 1, 1)
end_date = datetime(2024, 12, 31)

# ==============================
# 6. Ciclo para extraer y copiar mes a mes
# ==============================
for start_mes, end_mes in month_range(start_date, end_date):
    print(f"\n--- Procesando mes: {start_mes.strftime('%Y-%m')} ---")
    anio = start_mes.strftime('%Y')
    mes  = start_mes.strftime('%m')  # <-- Asegura formato 01,02,03...
    start_mes_str = start_mes.strftime('%d-%m-%Y')
    end_mes_str = end_mes.strftime('%d-%m-%Y')
    tabla_destino = f"dssge.dwe_centro_quirurgico_{anio}_{mes}"
    

    query = f"""
            SELECT
            t.INFOPEORICENASICOD									 								AS cod_oricentro,
            t.infopecenasicod                                        								AS cod_CENTRO,
            to_char(a.infopefec, 'yyyymm')                    		 								as PERIODO,
            to_char(a.infopefec, 'yyyy')                             								as ANIO,
            c.SOLOPEAREHOSCOD 										 								AS cod_area,
            c.SOLOPESERVHOSCOD 										 								AS cod_servicio, --de la solicitud
            t.infopecpscod                                           								AS COD_CPMS,
            k.infopepersopcod										 								AS COD_tip_cirujano,
            k.INFOPETIPDOCIDENPERCOD 								 								AS cod_tipdoc_medico,
            k.INFOPEPERASISDOCIDENNUM 								 								AS dni_medico,
            to_char(a.infopefec, 'dd/mm/yyyy')                       								AS FEC_OPER,
            e.perautcod                                              								AS AUTOGENERADO,
            d.actmedpacsecnum 										 								AS cmame_pacsecnum,
            e.PERTIPDOCIDENCOD										 								AS COD_TIPDOC_PACIENTE,
            e.perdocidennum                                          								AS DOC_PACIENTE,
            (FLOOR(MONTHS_BETWEEN(a.infopefec, e.pernacfec) / 12))                     				as ANIO_EDAD,       
            decode(e.persexocod, '1', 'M', '0', 'F', '')                               				as SEXO, 
            d.actmedtipsegcod                                        								AS COD_TIP_SEGURO,
            e.pertipoparecod                                         								as COD_TIPO_PARENTESCO,
            d.actmedtipopacicod  									 								AS COD_TIPO_PACIENTE,
            c.solopesalopecod                                        								AS COD_SALA,
            to_char(an.infopedursali,'HH24:MI')                      								AS HOR_INI_SALA,
            to_char(an.infopedursalf,'HH24:MI')                      								AS HOR_FIN_SALA,
            nvl(to_char(an.infopedursal,'HH24:MI'),to_char(a.infopeopetpo,'HH24:MI'))     			AS DURACION_SALA,
            to_char(an.infopeduranei,'HH24:MI')                                           			AS HOR_INI_ANEST,
            to_char(an.infopeduranef,'HH24:MI')                                           			AS HOR_FIN_ANEST,
            nvl(to_char(an.infopedurane,'HH24:MI'),to_char(a.infopeanetpo,'HH24:MI'))     			AS DURACION_ANEST,
            nvl(to_char(an.infopehiniope,'HH24:MI'),to_char(a.infopeingsophor,'HH24:MI')) 			AS HOR_INI_OPERAC,
            nvl(to_char(an.infopehfinope,'HH24:MI'),to_char(a.infopesoptpo,'HH24:MI'))    			AS HOR_FIN_OPERAC,
            nvl(to_char(an.infopehdurope,'HH24:MI'), to_char(an.infopehdurope,'HH24:MI')) 			AS DURACION_OPERAC,
            c.solopeactmedopenum                                     								AS acto_med,
            g.grdcmccod                                              								AS COD_COMPLEJIDAD,
            anes.infopeaneanecod                                     								AS COD_ANEST,
            c.conopecod 																			AS COD_TIPO_PROGRAMACION,
            c.solopenum                                                                      		AS NUM_SOLICITUD,
            c.solopecenquicod                                                                		AS COD_QUIROF,
            c.SolOpeSalOpeCod                                                                		AS COD_SALA_OPERACION,
            to_char(c.solopefec, 'dd/mm/yyyy')                                               		AS FECSOLICSALAOPERAC,
            to_CHAR(c.solopesolfec,'dd/mm/yyyy')                                             		AS FECSOLICITADAOPERAC,--07082019
            to_CHAR(c.solopeprofec,'dd/mm/yyyy')                                             		AS FECPROGRAM,  --07082019
            e.percenasiadscod                                                                		AS CAS_ADSCRIPCION,
            (select c.pachisclinum from sgss.cmpac10 c where c.oricenasicod = d.oricenasicod
            AND c.cenasicod = d.cenasicod and c.pacsecnum = d.actmedpacsecnum)               		AS H_C,
            a.desesocod																				AS COD_DESTEGRESO,
            to_char(c.solopecrefec,'dd/mm/yyyy')                                                  	AS FECHCREASOLICITUD,
            to_char(c.solopecrefec,'hh24:mi')                                                     	AS HORCREASOLIC,
            c.SolOpeEvalPQxFec                                                              		AS FECAPTITUD,
            c.solopetipeveope                                                                		AS COD_TIPO_EVENTO,
            a.InfOpeHallDes                                                                         AS DESC_HALLAZGO,
            a.InfOpeProcDes                                                                         AS DESC_PROCEDIMIENTO
            c.estopecod 																				AS COD_ESTADO_SOLICITUD, ----- suspendida = 3
            from sgss.qtioo10 t
            left outer join sgss.qtiop10 a on a.infopeoricenasicod = t.infopeoricenasicod
                                    and a.infopecenasicod    = t.infopecenasicod
                                    and a.infopesolopenum    = t.infopesolopenum
                                    and a.infopesecnum       = t.infopesecnum
            left outer join sgss.qtioc10 k on k.infopeoricenasicod = a.infopeoricenasicod
                                    and k.infopecenasicod    = a.infopecenasicod
                                    and k.infopesolopenum    = a.infopesolopenum
                                    and k.infopesecnum       = a.infopesecnum
                                    AND k.INFOPEPERORDNUM = '1'
            left outer join sgss.qtsop10 c on c.solopeoricenasicod  = a.infopeoricenasicod
                                    and c.solopecenasicod     = a.infopecenasicod
                                    and c.solopenum           = a.infopesolopenum
            left outer JOIN sgss.qtian10 an ON an.infopeoricenasicod = c.solopeoricenasicod
                                        AND an.infopecenasicod = c.solopecenasicod
                                        AND an.infopesolopenum = c.solopenum
                                        AND an.infopeanedesope <> '00'
            LEFT OUTER JOIN sgss.qtiaa10 anes ON anes.infopeoricenasicod = an.infopeoricenasicod
                                            AND anes.infopecenasicod    = an.infopecenasicod
                                            AND anes.infopesolopenum    = an.infopesolopenum
            left outer join sgss.cmame10 d on d.oricenasicod = c.solopeoricenasicod
                                    and d.cenasicod    = c.solopecenasicod
                                    and d.actmednum    = c.solopeactmednum
            left outer join sgss.cmper10 e on e.persecnum    = d.actmedpacsecnum
            left outer join sgss.cmcpp10 f on f.cpscod       = t.infopecpscod
            left outer join sgss.qbgcc10 g on g.grdcmccod    = f.grdcmccod
            where
            a.infopefec >= TO_DATE('{start_mes_str}', 'DD-MM-YYYY')
            and a.infopefec < TO_DATE('{end_mes_str}', 'DD-MM-YYYY')
            

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




