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
    limite = (end_date.replace(day=1) + relativedelta(months=1))
    while current < limite:
        start_mes = current
        end_mes = (current.replace(day=1) + relativedelta(months=1))
        yield start_mes, end_mes
        current = end_mes

# ==============================
# 1. Calcular rango: entre hace dos meses y el mes pasado
# ==============================
hoy = datetime.today()
#start_date = (hoy.replace(day=1) - relativedelta(months=2))  # Primer día del mes hace dos meses
#end_date = (hoy.replace(day=1) - relativedelta(months=1)) + relativedelta(day=31)  # Último día del mes pasado

start_time = datetime.now()
print(f"\n🕒 Inicio del ETL: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

start_date = datetime(2026, 7, 1)
end_date = datetime(2026, 7, 31)

# ==============================
# 6. Ciclo para extraer y copiar mes a mes
# ==============================
for start_mes, end_mes in month_range(start_date, end_date):
    anio = start_mes.strftime('%Y')
    mes = start_mes.strftime('%m')
    start_mes_str = start_mes.strftime('%d-%m-%Y')
    end_mes_str = end_mes.strftime('%d-%m-%Y')
    print(f"\n--- Procesando mes: {start_mes.strftime('%Y-%m')} ---")

    query = f"""
select
a.ATEPROORICENASICOD                  AS COD_ORICENTRO,
a.ATEPROCENASICOD                   AS COD_CENTRO,
to_char(a.ATEPROPROPERFEC,'yyyy') AS ANIO,
to_char(a.ATEPROPROPERFEC,'yyyymm') AS PERIODO,
b.servhoscod                         AS COD_SERVICIO,
a.ATEPROPERASISDOCIDENNUM           AS DNI_MEDICO,
p.grupocupcod                     AS GRUPO_OCUPACIONAL,
p.perespcod1                       AS ESPECIALIDAD_PER,
p.perespcod2                       AS ESPECIALIDAD2_PER,
s.perdocidennum                      AS DOC_PACIENTE,
s.pertipdocidencod 	                  AS TIP_DOC_PACIENTE,
TO_CHAR(FLOOR(MONTHS_BETWEEN(a.ATEPROPROPERFEC,s.pernacfec) / 12))          AS ANIO_EDAD,
decode(s.persexocod,'1','M','0','F','')                                      AS SEXO,
c.pachisclinum                                                               AS H_C,
m.tipsegcod                                                                  AS COD_TIPO_SEGURO,
s.pertipoparecod                                     AS COD_TIPO_PARENTESCO,
n.TIPOPACICOD                                                                 AS COD_TIPO_PACIENTE,
to_char(t.citambsolfec, 'dd/mm/yyyy')                                        AS FECHA_SOLIC,
to_char(a.ATEPROPROPERFEC, 'dd/mm/yyyy')                                    AS FECHA_ATEN,
t.condcitacod                       AS COD_CONDICION_CITA,
a.ATEPROACTMEDNUM                                                           AS ACTO_MED,
z.ATEPRCPSCOD                                                              AS CODPROCED,
z.ATEPRDCANT                                                                AS CANTPROCED,
z1.cenasioricod                                                              AS COD_PRECEDENCIA,
s.percenasiadscod                                                            AS CAS_ADSCRIPCION,
ct.concod                                                                    AS COD_CONSULTORIO,
t.citambactcod                                                               AS COD_ACTIVIDAD,
t.citambactespcod                                                            AS COD_SUBACTIVIDAD,
to_char(pr.properturhorini, 'hh24:mi')                                       AS horaini,
to_char(pr.properturhorfin, 'hh24:mi')                                       AS horafin,
to_char(pr.properturhorini, 'hh24:mi') ||'-'||
to_char(pr.properturhorfin, 'hh24:mi')                                       AS TURNO,
k.actmedestgrav                    AS TIPO_GRAVIDEZ,
s.perrucempnum                                                               AS NUM_RUC,
a.ATEPROUSUCRECOD                                                          AS USU_REG,  
to_char(a.ATEPROCREFEC,'dd/mm/yyyy')                                        AS FECH_REG,
to_char(a.ATEPROCREFEC,'hh24:mi')                                           AS HORA_REG,
a.ATEPROUSUMODCOD                                                           AS USU_MODIF,
to_char(a.ATEPROMODFEC,'dd/mm/yyyy')                                        AS FECH_MODIF,
to_char(a.ATEPROMODFEC,'hh24:mi')                                           AS HORA_MODIF,
a.ATEPROAREHOSCOD    AS AREA_HOSP
from SGSS.CTHPR10 a
left outer join SGSS.ctHPD10 z on z.ATEPROORICENASICOD = a.ATEPROORICENASICOD
                              and z.ATEPROCENASICOD   = a.ATEPROCENASICOD
                              and z.ATEPROACTMEDNUM         = a.ATEPROACTMEDNUM
                              AND z.ATEPRONUMSEC                       =a.ATEPRONUMSEC
left outer join SGSS.cmame10 k on z.ATEPROORICENASICOD = k.oricenasicod
                              and z.ATEPROCENASICOD   = k.cenasicod
                              and z.ATEPROACTMEDNUM   = k.actmednum
left outer join SGSS.cmtse10 m on k.actmedtipsegcod     = m.tipsegcod
left outer join SGSS.cmper10 s on k.actmedpacsecnum     = s.persecnum
left outer join SGSS.ctcam10 t on z.ATEPROORICENASICOD = t.citamboricenasicod
                              and z.ATEPROCENASICOD   = t.citambcenasicod
                              and z.ATEPROACTMEDNUM   = t.citambnum
left outer join SGSS.cmcpp10 f on  z.ATEPRCPSCOD              = f.cpscod
left outer join SGSS.cmsho10 b on a.ATEPROSERVHOSCOD          = b.servhoscod
left outer join SGSS.cbtpc10 n on k.actmedtipopacicod          = n.tipopacicod
left outer join SGSS.cmprs10 p on a.ATEPROTIPDOCIDENPERCOD    = p.tipdocidenpercod
                              and a.ATEPROPERASISDOCIDENNUM  = p.perasisdocidennum
left outer join SGSS.cmpac10 c on c.oricenasicod               = k.oricenasicod
                              and c.cenasicod                 = k.cenasicod
                              and c.pacsecnum                 = k.actmedpacsecnum
LEFT OUTER JOIN SGSS.ctref10 z1 ON k.actmedorirefnum                = z1.refnum
left outer join SGSS.ctpco10 ct on ct.proconoricenasicod = t.citambproconoricenasicod
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
left outer join SGSS.ctppe10 pr on pr.oricenasicod       = ct.proconoricenasicod
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
where a.ATEPROESTREGCOD    = '1'
   and a.ATEPROAREHOSCOD    = '02'
    AND a.ATEPROPROPERFEC >= TO_DATE('{start_mes_str}', 'DD-MM-YYYY')
    AND a.ATEPROPROPERFEC < TO_DATE('{end_mes_str}', 'DD-MM-YYYY')
    """

    print(f"Ejecutando query para mes {start_mes.strftime('%Y-%m')} en Oracle...")
    df = pd.read_sql(query, conn_oracle)
    print(f"Datos extraídos: {len(df)} filas.")

    if df.empty:
        print("No hay datos para este mes.")
        continue

    df.columns = df.columns.str.lower()

    # Truncar la tabla particionada destino en PostgreSQL antes de la carga
    tabla_particion = f"dssge.dw_proc_eme_{anio}_{mes}"
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
