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

start_time = datetime.now()
print(f"\n🕒 Inicio del ETL: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

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
start_date = datetime(2025, 10, 1)
end_date = datetime(2025, 10, 31)

# ==============================
# 6. Ciclo para extraer y copiar mes a mes
# ==============================
for start_mes, end_mes in month_range(start_date, end_date):
    print(f"\n--- Procesando mes: {start_mes.strftime('%Y-%m')} ---")
    anio = start_mes.strftime('%Y')
    mes  = start_mes.strftime('%m')  # <-- Asegura formato 01,02,03...
    tabla_destino = f"dssge.dw_consulta_externa_{anio}_{mes}"

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
    
    query = f"""
            select 
                a.ATENAMBORICENASICOD                                                        as COD_ORICENTRO,
                a.atenambcenasicod                                                           as COD_CENTRO,       
                to_char(a.atenambatenfec,'yyyymm')                                           as PERIODO,
                to_char(a.atenambatenfec,'yyyy')                                           as ANIO,
                to_char(a.atenambatenfec, 'hh24:mi')                                      as HORA_ATEN,
                t.CITAMBAREHOSCOD                                                        as cod_area,
                t.citambservhoscod                                                           AS COD_SERVICIO,       
                t.citambactcod                                                               AS COD_ACTIVIDAD,       
                t.citambactespcod                                                            AS COD_SUBACTIVIDAD,       
                a.ATENAMBCSECOD                                                          as cod_cartera,
                a.CPSCOD                                                                 as cod_cpms,              
                p.TIPDOCIDENPERCOD                                                       AS COD_TIPDOC_MEDICO,
                p.perasisdocidennum                                                                         as DNI_MEDICO,
                to_char(a.atenambatenfec,'dd/mm/yyyy')                                                     as FECHA_ATENCION,       
                s.perautcod                                                                                               as AUTOGENERADO,
                k.actmedpacsecnum                                                                                         as CMAME_PACSECNUM,
                s.pertipdocidencod                                                                                        AS COD_TIPDOC_PACIENTE,
                s.perdocidennum                                                                                           as DOC_PACIENTE,       
                (FLOOR(MONTHS_BETWEEN(a.atenambatenfec, s.pernacfec) / 12))                     as ANIO_EDAD,
                (FLOOR(MOD(MONTHS_BETWEEN(a.atenambatenfec, s.pernacfec), 12)))                 as MESES,       
                decode(s.persexocod, '1', 'M', '0', 'F', '')                                    as SEXO,       
                (select c.pachisclinum from sgss.cmpac10 c where c.oricenasicod = k.oricenasicod and
                c.cenasicod = k.cenasicod and c.pacsecnum = k.actmedpacsecnum)                             as H_C,
                k.actmedtipsegcod                                                                          AS COD_TIP_SEGURO,  
                --(select m.tipsegdes from cmtse10 m where m.tipsegcod = k.actmedtipsegcod)                  as TIPO_SEGURO,
                s.pertipoparecod                                                                           as COD_TIPO_PARENTESCO,
                --(select tp.tipoparenom from cbtpa10 tp WHERE tp.tipoparecod = s.pertipoparecod)            AS TIPO_PARENTESCO,
                k.actmedtipopacicod                                                                        AS COD_TIPO_PACIENTE,
                --(select n.tipopacinom from cbtpc10 n where k.actmedtipopacicod = n.tipopacicod)            as TIPO_PACIENTE,     
                to_char(t.citambsolfec, 'dd/mm/yyyy')                                                      as FECHA_SOLIC,
                to_char(t.citambproconfec, 'dd/mm/yyyy')                                                   as FECHA_CITA,
                to_char(a.ATENAMBCREFEC, 'dd/mm/yyyy')                                                     AS FECHA_REG,
                    to_char(a.ATENAMBCREFEC,'hh24:mi')                                                        AS HORA_REG,
                    to_char(a.atenambmodfec,'dd/mm/yyyy')                                                    AS FECHA_MODIF,
                a.atenambnum                                                                               as ACTO_MED,
                a.atenambtipconcod                                                                         as COD_TIPO_CONSULTA,
                --(select h.tipcondes from cmtco10 h where a.atenambtipconcod = h.tipconcod)                 as TIPO_CONSULTA,       
                --a.CPSCOD                                                                                   AS COD_CPMS,
                a.resatenambucod                                                                           AS COD_RESULT_ATENCION,
                --(select g.resatenambunom from cbraa10 g where a.resatenambucod = g.resatenambucod)         as DESC_RESULT_ATENCION,       
                k.actmedestgrav                                                                            as COD_TIPO_GRAVIDEZ,
                decode(k.actmedestpersercod, '1', 'N', '2', 'C', '3', 'R', '')                             as NRC_SER,
                decode(k.actmedestperestcod, '1', 'N', '2', 'C', '3', 'R', '')                              as NRC_EST, 
                a.CENASIREFCOD                                                                              as CAS_REFERENCIA,
                s.percenasiadscod                                                                           as CAS_ADSCRIPCION,
                ct.concod                                                                                   as COD_CONSULTORIO,      
                pr.properprohortot                                                                          as TOTAL_HORAS,
                pr.tipohorprogcod                                                  AS COD_TIP_PROGRAMACION,
                    pr.propertipohordet                                                as COD_TIPHORA,
                    to_char(pr.properturhorini, 'hh24:mi')                             AS HORAINI,
                    to_char(pr.properturhorfin, 'hh24:mi')                             AS HORAFIN,
                    to_char(pr.properturhorini, 'hh24:mi') ||'-'||
                    to_char(pr.properturhorfin, 'hh24:mi')                             AS TURNO,
                    pr.PROPERTIPOPROGPERSCOD                                           AS COD_TIP_PROGRAMACION_PERS,
                    pr.estprogcitcod                                                   AS COD_ESTADO_PROGRAMACION,   
                    pr.motsusprogcod                                                   AS COD_MOTIVO_SUSPENSION,
                p.perasisprocolcod                                                                                            AS CMP        
            FROM sgss.ctaam10 a
            LEFT OUTER JOIN sgss.cmcas10 ce on ce.cenasicod=a.atenambcenasicod
                                    and ce.oricenasicod = a.atenamboricenasicod
            LEFT OUTER JOIN sgss.cmras10 re on re.redasiscod=ce.redasiscod
            LEFT OUTER JOIN sgss.cmame10 k ON k.oricenasicod = a.atenamboricenasicod
                                    AND k.cenasicod    = a.atenambcenasicod
                                    AND k.actmednum    = a.atenambnum
            LEFT OUTER JOIN sgss.cmper10 s ON k.actmedpacsecnum    = s.persecnum
            LEFT OUTER JOIN sgss.ctcam10 t ON t.citamboricenasicod = a.atenamboricenasicod
                                    AND t.citambcenasicod    = a.atenambcenasicod
                                    AND t.citambnum          = a.atenambnum
            left outer join sgss.ctpco10 ct on ct.proconoricenasicod = t.citambproconoricenasicod
                                    and ct.proconcenasicod   = t.citambcenasicod
                                    and ct.proconarehoscod   = t.citambarehoscod
                                    and ct.proconservhoscod  = t.citambservhoscod
                                    and ct.proconactcod      = t.citambactcod
                                    and ct.proconactespcod   = t.citambactespcod
                                    and ct.procontipdocidenpercod = t.citambtipdocidenpercod
                                    and ct.proconperasisdocidennum = t.citambperasisdocidennum
                                    and ct.proconfec               = t.citambproconfec
                                    and ct.proconturhorini         = t.citambproconturhorini
                                    and ct.proconturhorfin         = t.citambproconturhorfin
            left outer join sgss.ctppe10 pr on pr.oricenasicod       = ct.proconoricenasicod
                                    and pr.cenasicod          = ct.proconcenasicod
                                    and pr.arehoscod          = ct.proconarehoscod
                                    and pr.servhoscod         = ct.proconservhoscod
                                    and pr.actcod             = ct.proconactcod
                                    and pr.actespcod          = ct.proconactespcod
                                    and pr.tipdocidenpercod   = ct.procontipdocidenpercod
                                    and pr.perasisdocidennum  = ct.proconperasisdocidennum
                                    and pr.properfec          = ct.proconfec
                                    and pr.properturhorini    = ct.proconturhorini
                                    and pr.properturhorfin    = ct.proconturhorfin
            LEFT OUTER JOIN sgss.cmprs10 p ON t.citambtipdocidenpercod  = p.tipdocidenpercod
                                    AND t.citambperasisdocidennum = p.perasisdocidennum
            LEFT OUTER JOIN sgss.ctref10 z ON k.actmedorirefnum         = z.refnum
            LEFT OUTER JOIN sgss.CMPAC10 t1 ON  t1.oricenasicod          = k.oricenasicod
                                    AND t1.cenasicod             = k.cenasicod
                                    AND t1.pacsecnum             = k.actmedpacsecnum
            WHERE
            a.atenambestregcod = '1'  
            and a.atenambatenfec >= TO_DATE('{start_mes.strftime('%d-%m-%Y')}', 'DD-MM-YYYY')
            and a.atenambatenfec < TO_DATE('{(end_mes + timedelta(days=1)).strftime('%d-%m-%Y')}', 'DD-MM-YYYY')

    """

    print(f"Ejecutando query para mes {start_mes.strftime('%Y-%m')} en Oracle...")
    df = pd.read_sql(query, conn_oracle)
    print(f"Datos extraídos: {len(df)} filas.")

    if df.empty:
        print("No hay datos para este mes.")
        continue

    df.columns = df.columns.str.lower()




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




