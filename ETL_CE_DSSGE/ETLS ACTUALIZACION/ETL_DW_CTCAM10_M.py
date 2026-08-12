import os
import oracledb
import polars as pl
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

start_time = datetime.now()
print(f"\n🕒 Inicio del ETL: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

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

start_date = datetime(2026, 8, 1)
end_date = datetime(2026, 8, 30)


# ==============================
# 6. Ciclo para extraer y copiar mes a mes
# ==============================
for start_mes, end_mes in month_range(start_date, end_date):
    mes_start_time = datetime.now()
    anio = start_mes.strftime('%Y')
    mes = start_mes.strftime('%m')
    start_mes_str = start_mes.strftime('%d-%m-%Y')
    end_mes_str = end_mes.strftime('%d-%m-%Y')
    print(f"\n--- Procesando mes: {start_mes.strftime('%Y-%m')} ---")
    print(f"🕒 Inicio del mes: {mes_start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    query = f"""
    SELECT to_char(citambproconfec, 'yyyy') as anio,
           to_char(citambproconfec, 'yyyymm') as periodo,
           citambproconoricenasicod as cod_oricentro,
           citambproconcenasicod  as cod_centro,
           citambnum as acto_med, 
           citambpacsecnum as pac_sec,
           tipocitacod as cod_tipo_cita,
           EstCitOtoCod AS cod_cita_otorgar,
           motelicitcod as cod_mot_eli,
           citambsolfec as fecha_solicitud,
           citambcrefec as fecha_creacion,
           citambarehoscod as cod_area,
           citambservhoscod as cod_servicio,
           citambactcod as cod_actividad,
           citambactespcod as cod_subactividad,
           citambtipdocidenpercod as cod_tipdoc_medico,
           citambperasisdocidennum as cod_doc_medico,
           citambproconfec as fecha_cita,
           citambmodfec as fecha_modificacion,
           case 
               when citambrep = '1' then citambmodfec 
               else citambproconfec end as fecha_ultima_modificacion, 
           citambproconturhorini as horaini,
           citambproconturhorfin as horafin,
           estcitcod as cod_estado_cita,
           n.tipopacicod as cod_tipo_paciente,
            d.perdocidennum                     as doc_paciente,
            d.PERTIPDOCIDENCOD                 as cod_tipdoc_paciente,
            k.actmededadaten                    as anio_edad,
            decode(d.persexocod, '1', 'M', '0', 'F', '')       AS sexo

    FROM SGSS.ctcam10
    left outer join sgss.cmame10 k on citamboricenasicod  = k.oricenasicod
                         and citambcenasicod     = k.cenasicod
                         and citambnum           = k.actmednum
    left outer join sgss.cmper10 d on k.actmedpacsecnum       = d.persecnum                         
    left outer join sgss.cbtpc10 n on k.actmedtipopacicod       = n.tipopacicod
        WHERE citambproconfec >= TO_DATE('{start_mes_str}', 'DD-MM-YYYY')
            AND citambproconfec < TO_DATE('{end_mes_str}', 'DD-MM-YYYY')
    ORDER BY periodo ASC
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

    # Truncar la tabla particionada destino en PostgreSQL antes de la carga
    tabla_particion = f"dssge.sgss_ctcam10_m_{anio}_{mes}"
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
    df.write_csv(
        csv_buffer,
        include_header=False,
        datetime_format="%Y-%m-%d %H:%M:%S",
        date_format="%Y-%m-%d",
    )
    csv_buffer.seek(0)

    # Usamos COPY para cargar datos a PostgreSQL
    print(f"Cargando datos a PostgreSQL en tabla {tabla_particion}...")
    carga_start = datetime.now()
    try:
        cursor_pg.copy_expert(
            sql=f"COPY {tabla_particion} ({', '.join(df.columns)}) FROM STDIN WITH CSV",
            file=csv_buffer
        )
        conn_pg.commit()
        carga_time = datetime.now() - carga_start
        print(f"Mes {start_mes.strftime('%Y-%m')} cargado correctamente. (tiempo de carga: {carga_time})")
    except Exception as e:
        print(f"Error al cargar mes {start_mes.strftime('%Y-%m')}: {e}")
        continue

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


