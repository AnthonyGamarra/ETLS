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
start_date = datetime(2026, 6, 1)
end_date = datetime(2026, 6, 30)

# ==============================
# 6. Ciclo para extraer y copiar mes a mes
# ==============================
for start_mes, end_mes in month_range(start_date, end_date):
    print(f"\n--- Procesando mes: {start_mes.strftime('%Y-%m')} ---")
    anio = start_mes.strftime('%Y')
    mes  = start_mes.strftime('%m')  # <-- Asegura formato 01,02,03...
    start_mes_str = start_mes.strftime('%d-%m-%Y')
    end_mes_str = end_mes.strftime('%d-%m-%Y')
    tabla_destino = f"dssge.dw_hosp_{anio}_{mes}"
    

    query = f"""
    SELECT
            b.ORICENASICOD                                                     as COD_ORICENTRO,
            b.cenasicod                                                        AS COD_CENTRO,
            to_char(trunc(t.hospaltadmfec),'yyyymm')                           as PERIODO,
            to_char(trunc(t.hospaltadmfec),'yyyy')                             as ANIO,
            ho.ATENHOSSERVHOSCOD                                               AS COD_SERVICIO,                                             
            ho.ATENHOSACTCOD                                                   AS COD_ACTIVIDAD,
            u.hosdarehosintcod                                                 AS COD_AREA_INTER,
            u.hosdservhoscod                                                   AS COD_SERV_INTER,
            e1.estenfcod                                                       AS COD_ESTACION,
            p.perasisprocolcod                                                 AS CMP,
            ho.ATENHOSTIPDOCIDENPERCOD                                         as COD_TIPDOC_MEDICO,
            ho.ATENHOSPERASISDOCIDENNUM                                        as DNI_MEDICO,
            d.perautcod                                                        as AUTOGENERADO,
            a.actmedpacsecnum                                                  as CMAME_PACSECNUM,
            d.pertipdocidencod                                                 AS COD_TIPDOC_PACIENTE,
            d.perdocidennum                                                    as DOC_PACIENTE,
            (FLOOR(MONTHS_BETWEEN(t.hospaltadmfec, d.pernacfec) / 12))         as ANIO_EDAD,
            (FLOOR(MOD(MONTHS_BETWEEN(t.hospaltadmfec, d.pernacfec), 12)))     as MESES,       
            decode(d.persexocod, '1', 'M', '0', 'F', '')                       as SEXO,
            e.pachisclinum                                                     as NRO_HC,
            a.actmedtipsegcod                                                  AS COD_TIP_SEGURO,  
            d.pertipoparecod                                                   as COD_TIPO_PARENTESCO,
            a.actmedtipopacicod                                                AS COD_TIPO_PACIENTE,                      
            to_char(t.hospintfec,'dd/mm/yyyy')                                 as FEC_INGR,
            to_char(t.hospinthor,'hh24:mi')                                    as HOR_INGR,
            to_char(trunc(t.hospaltadmfec), 'dd/mm/yyyy')                      as FEC_ALTMED,
            to_char(t.hospaltadmhor,'hh24:mi')                                 as HOR_ALTMED,
            to_char(trunc(t.hospaltadmfec), 'dd/mm/yyyy')                      as FEC_EGRESO,
            to_char(t.hospaltadmhor,'hh24:mi')                                 as HOR_EGRESO,
            to_char(t.hospcrefec,'dd/mm/yyyy')                                 AS FECH_REG,
            to_char(t.hospmodfec,'dd/mm/yyyy hh24:mi')                         AS FECH_MODIF,
            CASE WHEN (to_char(t.hospintfec, 'dd/mm/yyyy') = '01/01/0001'
            or t.hospintfec is NULL
            or to_char(t.hospaltadmfec, 'dd/mm/yyyy') ='01/01/0001'
            or t.hospaltadmfec is NULL)
            THEN
            trunc((t.hospaltadmfec - t.hospcrefec))
            ELSE
            CASE WHEN (t.hospaltadmfec - t.hospintfec) > 0
            THEN
                (t.hospaltadmfec - t.hospintfec)
            ELSE
                1
            END
            END                                                                             AS DIAS_HOSPIT,
            t.hospactmednum                                                                 as ACTO_MED,
            a.actmedestgrav                                                                 as COD_TIPO_GRAVIDEZ,
            t.motegrcod                                                                     as COD_MOTIVO_EGRESO,
            u.hosdhabcod                                                                    as CUARTO, -- 100719
            u.hosdcamcod                                                                    as CAMA,
            cam.TIPCAMCOD                                                                   AS COD_TIPO_CAMA,
            d.percenasiadscod                                                               as CAS_ADSCRIPCION,
            p.GrupOcupCod                                                                   as COD_GRUPOCUP_ALTAMED,
            (SELECT MAX(v1.ATENHOSNUMSEC) FROM sgss.htaho10 v1
            WHERE v1.atenhosoricenasicod = ho.ATENHOSORICENASICOD
            AND v1.atenhoscenasicod    = ho.ATENHOSCENASICOD
            AND v1.atenhosactmednum    = ho.ATENHOSACTMEDNUM)                                 AS ULTSECUENATEN,
            decode(cam.camflgfun, '0','HOSPITALIZACION','1','UCI','2','UCIN','3','UVI','4','EMERGENCIA','5',
            'URGENCIA','6','SHOCK_TRAUMA','7','ALOJAMIENTO-CONJUNTO','8','RECU-CQX','9', 'CENTRO_OBSTETRICO','A','CRIP','B', 'HOSPIT_CONTRATADA') AS UBICACION,
            t.HOSPEGRORICENASIREFCOD                                        as COD_CENTRO_REF,
            t.HOSPEGRCENASIREFCOD                                           AS CENTRO_REF,
            u1.AMeAreHosCod                                                          AS AREA_ORIGEN
            from sgss.hthos10 t
            inner join sgss.hthod10 u on u.hosporicenasicod = t.hosporicenasicod
                                and u.hospcenasicod    = t.hospcenasicod
                                and u.hospactmednum     = t.hospactmednum
                                and u.hosdnumsec = (select max(v.hosdnumsec) from sgss.hthod10 v
                                                    where u.hosporicenasicod = v.hosporicenasicod
                                                    and u.hospcenasicod    = v.hospcenasicod
                                                    and u.hospactmednum    = v.hospactmednum)
            inner join sgss.htaho10 ho on ho.ATENHOSORICENASICOD = t.HOSPORICENASICOD
                                and ho.ATENHOSCENASICOD = t.HOSPCENASICOD
                                and ho.ATENHOSACTMEDNUM = t.HOSPACTMEDNUM
                                and ho.ATENHOSNUMSEC = (SELECT MAX(v1.ATENHOSNUMSEC) FROM sgss.htaho10 v1
                                                        WHERE v1.atenhosoricenasicod = ho.ATENHOSORICENASICOD
                                                        AND v1.atenhoscenasicod    = ho.ATENHOSCENASICOD
                                                        AND v1.atenhosactmednum    = ho.ATENHOSACTMEDNUM)
            left outer join sgss.htoin10 u1 on u1.OrdIntOriCenAsiCod = t.hosporicenasicod
                                and u1.OrdIntCenAsiCod    = t.hospcenasicod
                                and u1.OrdIntNum     = t.hospactmednum
            LEFT OUTER join sgss.HMESE10 e1 on e1.oricenasicod = u.hosporicenasicod
                                and e1.cenasicod = u.hospcenasicod
                                and e1.arehoscod = u.hosdarehosintcod
                                and e1.servhoscod = u.hosdservhoscod
                                and e1.estenfcod = u.hosdestenfcod
            left outer join sgss.cmame10 a on t.hosporicenasicod  = a.oricenasicod
                                    and t.hospcenasicod    = a.cenasicod
                                    and t.hospactmednum    = a.actmednum

            left outer join sgss.cmcas10 b on t.hosporicenasicod      = b.oricenasicod
                                    and t.hospcenasicod        = b.cenasicod
            left outer join sgss.cmsho10 c on u.hosdserhosintcod      = c.servhoscod
            left outer join sgss.cmper10 d on a.actmedpacsecnum       = d.persecnum
            LEFT OUTER JOIN sgss.cmprs10 p ON p.tipdocidenpercod      = ho.ATENHOSTIPDOCIDENPERCOD
                                    AND p.perasisdocidennum    = ho.ATENHOSPERASISDOCIDENNUM
            left outer join sgss.cmpac10 e on a.oricenasicod         = e.oricenasicod
                                    and a.cenasicod            = e.cenasicod
                                    and a.actmedpacsecnum      = e.pacsecnum
            LEFT OUTER JOIN sgss.nmusu10 ur ON ur.usecod             = t.hospusucrecod
            inner join sgss.hmcam10 cam on u.HOSPORICENASICOD = cam.ORICENASICOD
                                and u.HOSPCENASICOD = cam.CENASICOD
                                and u.HOSDAREHOSCOD = cam.AREHOSCOD
                                and u.HOSDSERVHOSCOD = cam.SERVHOSCOD
                                and u.HOSDESTENFCOD = cam.ESTENFCOD
                                and u.HOSDHABCOD = cam.HABCOD
                                and u.HOSDCAMCOD = cam.CAMCOD
            inner join sgss.hbtca10 tcam on cam.TIPCAMCOD = tcam.tipcamcod
            where t.hospegrflg = '1'
            and t.hospaltadmfec >= TO_DATE('{start_mes_str}', 'DD-MM-YYYY')
            and t.hospaltadmfec < TO_DATE('{end_mes_str}', 'DD-MM-YYYY')
            AND t.motegrcod IN ('01','02','04','05','11','12','13','14')
            AND u.estpehcod = '3'
            AND t.hosparehoscod    = '03'
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