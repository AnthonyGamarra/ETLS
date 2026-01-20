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
# Función para generar rangos mensuales
# ==============================
def month_range(start_date, end_date):
    current = start_date
    while current <= end_date:
        start_mes = current
        end_mes = (current + relativedelta(months=1)) - timedelta(days=1)
        yield start_mes, end_mes
        current += relativedelta(months=1)

# ==============================
# 1. Calcular rango: entre hace dos meses y el mes pasado
# ==============================
hoy = datetime.today()
#start_date = (hoy.replace(day=1) - relativedelta(months=2))  # Primer día del mes hace dos meses
#end_date = (hoy.replace(day=1) - relativedelta(months=1)) + relativedelta(day=31)  # Último día del mes pasado

start_date = datetime(2026, 1, 1)
end_date = datetime(2026, 1, 31)

# ==============================
# 6. Ciclo para extraer y copiar mes a mes
# ==============================
for start_mes, end_mes in month_range(start_date, end_date):
    anio = start_mes.strftime('%Y')
    mes = start_mes.strftime('%m')
    print(f"\n--- Procesando mes: {start_mes.strftime('%Y-%m')} ---")

    query = f"""
select
t.citamboricenasicod                                           AS COD_ORICENTRO,
t.citambcenasicod                                              AS COD_CENTRO,
to_char(z.ATENODOATENFEC,'YYYYMM')                             AS PERIODO,
to_char(z.ATENODOATENFEC,'YYYY')                               AS ANIO,
to_char(z.ATENODOATENFEC,'hh24:mi')                            AS HORA_ATEN,
t.CITAMBAREHOSCOD                                              as COD_AREA,
t.citambservhoscod                                             AS COD_SERVICIO,       
t.citambactcod                                                 AS COD_ACTIVIDAD,       
t.citambactespcod                                              AS COD_SUBACTIVIDAD, 
z.ATENODOCSECOD                                                AS COD_CARTERA, 
z.CPSCOD                                                       AS COD_CPMS,
t.CITAMBTIPDOCIDENPERCOD                                       AS COD_TIPDOC_MEDICO,
t.citambperasisdocidennum                                      AS DNI_MEDICO,
to_char(z.ATENODOATENFEC,'dd/mm/yyyy')                          AS FECHA_ATENCION,
s.perautcod                                                    AS AUTOGENERADO,
k.actmedpacsecnum                                              AS CMAME_PACSECNUM,
s.pertipdocidencod                                             AS COD_TIPDOC_PACIENTE,
s.perdocidennum                                                AS DOC_PACIENTE,
(FLOOR(MONTHS_BETWEEN(t.citambproconfec,s.pernacfec)/12))      AS ANIO_EDAD,
(FLOOR(MOD(MONTHS_BETWEEN(t.citambproconfec,s.pernacfec),12))) AS MESES,
decode(s.persexocod,'1','M','0','F','')                        AS SEXO,
c.pachisclinum                                                 AS H_C,
 k.actmedtipsegcod                                             AS COD_TIP_SEGURO, 
s.pertipoparecod                                               as COD_TIPO_PARENTESCO,
k.actmedtipopacicod                                            AS COD_TIPO_PACIENTE,
to_char(t.citambsolfec, 'dd/mm/yyyy')                          AS FECHA_SOLIC,
to_char(t.citambproconfec, 'dd/mm/yyyy')                       AS FECHA_CITA,
to_char(z.ATENODOCREFEC, 'dd/mm/yyyy')                         AS FECHA_REG,
to_char(z.ATENODOCREFEC,'hh24:mi')                             AS HORA_REG,
to_char(z.ATENODOMODFEC,'dd/mm/yyyy')                          AS FECHA_MODIF,
t.citambnum                                                    AS ACTO_MED,
z.ATENODOTIPCONCOD                                             AS COD_TIPO_CONSULTA,
t.condcitacod                                                  AS COD_CONDICION_CITA,
z.RESATENODOCOD                                                AS COD_RESULT_ATENCION,
k.actmedestgrav                                                AS COD_TIPO_GRAVIDEZ,
decode(k.actmedestpersercod, '1', 'N', '2', 'C', '3', 'R', '') AS NRC_SER,
decode(k.actmedestperestcod, '1', 'N', '2', 'C', '3', 'R', '') AS NRC_EST,
z.CENASIREFCOD                                                 AS CAS_REFERENCIA,
s.percenasiadscod                                              AS CAS_ADSCRIPCION,
ct.concod                                                      AS COD_CONSULTORIO,
pr.properprohortot                                             AS TOTAL_HORAS,
pr.tipohorprogcod                                              AS COD_TIP_PROGRAMACION,
pr.propertipohordet                                            AS COD_TIPHORA,
to_char(pr.properturhorini, 'hh24:mi')                         AS HORAINI,
to_char(pr.properturhorfin, 'hh24:mi')                         AS HORAFIN,
to_char(pr.properturhorini, 'hh24:mi') ||'-'||
to_char(pr.properturhorfin, 'hh24:mi')                         AS TURNO,
pr.PROPERTIPOPROGPERSCOD                                       AS COD_TIP_PROGRAMACION_PERS,
pr.estprogcitcod                                               AS COD_ESTADO_PROGRAMACION,   
pr.motsusprogcod                                               AS COD_MOTIVO_SUSPENSION
from sgss.CTCAM10 t
INNER JOIN     sgss.cmame10 k on t.citamboricenasicod = k.oricenasicod
                        and t.citambcenasicod    = k.cenasicod
                        and t.citambnum          = k.actmednum
                        and K.ACTMEDATE          = '1' 
INNER JOIN sgss.ctaod10 z on t.citamboricenasicod = z.atenodooricenasicod
                         and t.citambcenasicod    = z.atenodocenasicod
                         and t.citambnum          = z.atenodonum
left outer join sgss.cmper10 s on k.actmedpacsecnum    = s.persecnum
left outer join sgss.cmprs10 p on t.citambtipdocidenpercod  = p.tipdocidenpercod
                         and t.citambperasisdocidennum = p.perasisdocidennum
left outer join sgss.cmpac10 c on c.oricenasicod = k.oricenasicod
                         and c.cenasicod    = k.cenasicod
                         and c.pacsecnum    = k.actmedpacsecnum
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
    WHERE z.ATENODOATENFEC >= TO_DATE('{start_mes.strftime('%d-%m-%Y')}', 'DD-MM-YYYY')
      AND z.ATENODOATENFEC <= TO_DATE('{end_mes.strftime('%d-%m-%Y')}', 'DD-MM-YYYY')
    AND t.estcitcod         = '4'
    ORDER BY periodo ASC
    """

    print(f"Ejecutando query para mes {start_mes.strftime('%Y-%m')} en Oracle...")
    df = pd.read_sql(query, conn_oracle)
    print(f"Datos extraídos: {len(df)} filas.")

    if df.empty:
        print("No hay datos para este mes.")
        continue

    df.columns = df.columns.str.lower()

    # Truncar la tabla particionada destino en PostgreSQL antes de la carga
    tabla_particion = f"dssge.dw_odonto_{anio}_{mes}"
    try:
        print(f"Truncando tabla particionada destino: {tabla_particion}...")
        cursor_pg.execute(f"TRUNCATE TABLE {tabla_particion};")
        conn_pg.commit()
        print(f"Tabla {tabla_particion} truncada correctamente.")
    except Exception as e:
        print(f"Error al truncar la tabla {tabla_particion}: {e}")
        continue

    # Guardamos el DataFrame en un buffer CSV en memoria
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)

    # Usamos COPY para cargar datos a PostgreSQL
    print(f"Cargando datos a PostgreSQL en tabla {tabla_particion}...")
    try:
        cursor_pg.copy_expert(
            sql=f"COPY {tabla_particion} ({', '.join(df.columns)}) FROM STDIN WITH CSV",
            file=csv_buffer
        )
        conn_pg.commit()
        print(f"Mes {start_mes.strftime('%Y-%m')} cargado correctamente.")
    except Exception as e:
        print(f"Error al cargar mes {start_mes.strftime('%Y-%m')}: {e}")
        continue

# ==============================
# 7. Cerramos conexiones
# ==============================
print("\nCerrando conexiones a bases de datos...")
cursor_pg.close()
conn_pg.close()
conn_oracle.close()
print("Conexiones cerradas. Proceso finalizado.")
