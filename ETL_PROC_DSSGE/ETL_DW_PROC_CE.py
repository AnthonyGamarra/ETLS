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

start_time = datetime.now()
print(f"\n🕒 Inicio del ETL: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

#start_date = (hoy.replace(day=1) - relativedelta(months=2))  # Primer día del mes hace dos meses
#end_date = (hoy.replace(day=1) - relativedelta(months=1)) + relativedelta(day=31)  # Último día del mes pasado

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
a.atenprooricenasicod                   AS COD_ORICENTRO,
a.atenprocenasicod                   AS COD_CENTRO,
to_char(z.atenproproperfec,'yyyy') AS ANIO,
to_char(z.atenproproperfec,'yyyymm') AS PERIODO,
b.servhoscod                         AS COD_SERVICIO,
a.atenproperasisdocidennum           AS DNI_MEDICO,
p.grupocupcod                     AS GRUPO_OCUPACIONAL,
p.perespcod1                       AS ESPECIALIDAD_PER,
p.perespcod2                       AS ESPECIALIDAD2_PER,
s.perdocidennum                      AS DOC_PACIENTE,
s.pertipdocidencod 	                  AS TIP_DOC_PACIENTE,
(FLOOR(MONTHS_BETWEEN(z.atenproproperfec,s.pernacfec) / 12))                 AS ANIO_EDAD,
decode(s.persexocod,'1','M','0','F','')                                      AS SEXO,
c.pachisclinum                                                               AS H_C,
m.tipsegcod                                                                  AS COD_TIPO_SEGURO,
s.pertipoparecod                                     AS COD_TIPO_PARENTESCO,
n.TIPOPACICOD                                                                 AS COD_TIPO_PACIENTE,
to_char(t.citambsolfec, 'dd/mm/yyyy')                                        AS FECHA_SOLIC,
to_char(z.atenproproperfec, 'dd/mm/yyyy')                                    AS FECHA_ATEN,
t.condcitacod                       AS COD_CONDICION_CITA,
z.atenproactmednum                                                           AS ACTO_MED,
a.atenprcpscod                                                               AS CODPROCED,
a.atenprdcant                                                                AS CANTPROCED,
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
decode(a.atenprdtipenf,'0','PACIENTE NO COVID','1','PACIENTE COVID')         AS ESTADO_PACIENTE, 
a.atenprdusucrecod                                                           AS USU_REG,  
to_char(a.atenprdcrefec,'dd/mm/yyyy')                                        AS FECH_REG,
to_char(a.atenprdcrefec,'hh24:mi')                                           AS HORA_REG,
a.atenprdusumodcod                                                           AS USU_MODIF,
to_char(a.atenprdmodfec,'dd/mm/yyyy')                                        AS FECH_MODIF,
to_char(a.atenprdmodfec,'hh24:mi')                                           AS HORA_MODIF,
a.atenproarehoscod    AS AREA_HOSP
from SGSS.ctapd10 a
left outer join SGSS.ctapr10 z on z.atenprooricenasicod = a.atenprooricenasicod
                              and z.atenprocenasicod   = a.atenprocenasicod
                              and z.atenproarehoscod   = a.atenproarehoscod
                              and z.atenproservhoscod  = a.atenproservhoscod
                              and z.atenproactcod      = a.atenproactcod
                              and z.atenproactespcod   = a.atenproactespcod
                              and z.atenprotipdocidenpercod   = a.atenprotipdocidenpercod
                              and z.atenproperasisdocidennum  = a.atenproperasisdocidennum
                              and z.atenproproperfec          = a.atenproproperfec
                              and z.atenproturinihor          = a.atenproturinihor
                              and z.atenproturfinhor          = a.atenproturfinhor
                              and z.atenproactmednum          = a.atenproactmednum
left outer join SGSS.cmame10 k on z.atenprooricenasicod = k.oricenasicod
                              and z.atenprocenasicod   = k.cenasicod
                              and z.atenproactmednum   = k.actmednum
left outer join SGSS.cmtse10 m on k.actmedtipsegcod     = m.tipsegcod
left outer join SGSS.cmper10 s on k.actmedpacsecnum     = s.persecnum
left outer join SGSS.ctcam10 t on z.atenprooricenasicod = t.citamboricenasicod
                              and z.atenprocenasicod   = t.citambcenasicod
                              and z.atenproactmednum   = t.citambnum
left outer join SGSS.cmcpp10 f on  a.atenprcpscod              = f.cpscod
left outer join SGSS.cmsho10 b on a.atenproservhoscod          = b.servhoscod
left outer join SGSS.cbtpc10 n on k.actmedtipopacicod          = n.tipopacicod
left outer join SGSS.cmprs10 p on a.atenprotipdocidenpercod    = p.tipdocidenpercod
                              and a.atenproperasisdocidennum  = p.perasisdocidennum
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
where z.atenproestregcod    = '1'
    and a.atenproarehoscod    in ('01','02','03','04','05','06')
    AND z.atenproproperfec >= TO_DATE('{start_mes_str}', 'DD-MM-YYYY')
    AND z.atenproproperfec < TO_DATE('{end_mes_str}', 'DD-MM-YYYY')
    """

    print(f"Ejecutando query para mes {start_mes.strftime('%Y-%m')} en Oracle...")
    df = pd.read_sql(query, conn_oracle)
    print(f"Datos extraídos: {len(df)} filas.")

    if df.empty:
        print("No hay datos para este mes.")
        continue

    df.columns = df.columns.str.lower()

    # Truncar la tabla particionada destino en PostgreSQL antes de la carga
    tabla_particion = f"dssge.dw_proc_{anio}_{mes}"
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