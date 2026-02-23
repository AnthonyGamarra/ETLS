import os
import psycopg2
import pandas as pd
import io
from dotenv import load_dotenv
from datetime import datetime

# Cargar variables de entorno
load_dotenv()

# Configuración de base de datos origen y destino
pg_user = os.getenv("PG_USER")
pg_pass = os.getenv("PG_PASS")
pg_host = os.getenv("PG_HOST")
pg_db   = os.getenv("PG_DB")

# Crear conexiones (las abrimos una vez fuera del loop)
conn_src = psycopg2.connect(
    dbname=pg_db,
    user=pg_user,
    password=pg_pass,
    host=pg_host,
    port=5433
)

conn_dst = psycopg2.connect(
    dbname=pg_db,
    user=pg_user,
    password=pg_pass,
    host=pg_host,
    port=5433
)

anio = datetime.now().year 
start_time = datetime.now()
today = datetime.today()
current_year = today.year
current_month = today.month
time= datetime.now().strftime("'%Y-%m-%d %H:%M:%S'")

# Calcular los dos meses anteriores
if current_month == 1:
    months_to_process = [(current_year - 1, 11), (current_year - 1, 12)]
elif current_month == 2:
    months_to_process = [(current_year - 1, 12), (current_year, 1)]
else:
    months_to_process = [(current_year, current_month - 2), (current_year, current_month - 1)]

print(f"\nInicio del ETL: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

for mes in range(2, 3):
    mes_str = f"{mes:02d}"

    print(f"\n📆 Procesando mes: {anio}-{mes_str}")

    try:
        query = f"""
                WITH atenciones AS (
                    SELECT
                        u.cod_oricentro,
                        u.cod_centro,
                        u.cod_area,
                        u.cod_servicio,
                        u.cod_actividad,
                        u.cod_subactividad,
                        u.cod_tipdoc_medico,
                        u.cod_doc_medico,
                        u.fecha_cita,
                        u.horaini,
                        u.horafin,
                        COUNT(*)::integer AS ate           
                    FROM dssge.sgss_ctcam10_m_{anio}_{mes_str} u
                    WHERE u.cod_estado_cita = '4'
                    GROUP BY
                        u.cod_oricentro,
                        u.cod_centro,
                        u.cod_area,
                        u.cod_servicio,
                        u.cod_actividad,
                        u.cod_subactividad,
                        u.cod_tipdoc_medico,
                        u.cod_doc_medico,
                        u.fecha_cita,
                        u.horaini,
                        u.horafin
                ),

                base AS (
                    SELECT
                        t.anio,
                        t.periodo,
                        t.oricenasicod AS cod_oricentro,
                        t.cenasicod AS cod_centro,
                        t.arehoscod AS cod_area,
                        t.actcod AS cod_actividad,
                        t.actespcod AS cod_subactividad,
                        t.servhoscod AS cod_servicio,
                        e.cod_especialidad,
                        e.cod_subespecialidad,
                        e.cod_agrupador,
                        e.cod_variable,
                        t.properprohortot::numeric(10,2) AS total_horas,
                        t.perasisdocidennum AS dni_medico,
                        t.motsusprogcod AS cod_mot_suspension,
                        COALESCE(a.ate,0)::integer AS ate, 
                        t.properfec AS fecha_prog,
                        (
                            CASE
                                WHEN CAST(t.properturhorfin AS time) >= CAST(t.properturhorini AS time)
                                THEN EXTRACT(EPOCH FROM (
                                        CAST(t.properturhorfin AS time)
                                    - CAST(t.properturhorini AS time)
                                    ))
                                ELSE EXTRACT(EPOCH FROM (
                                        CAST(t.properturhorfin AS time)
                                    + INTERVAL '24 hours'
                                    - CAST(t.properturhorini AS time)
                                    ))
                            END
                        ) / 3600.0
                        ::numeric(10,2) AS hras_prog,

                        t.estprogcitcod AS estado_progcita

                    FROM dssge.sgss_ctppe10_{anio}_{mes_str} t

                    LEFT JOIN dssge.sgss_cmprs10 pr
                        ON pr.tipdocidenpercod = t.tipdocidenpercod
                    AND pr.perasisdocidennum = t.perasisdocidennum

                    LEFT JOIN dssge.dw_homologacion_enlaces e
                        ON t.actcod::text = e.cod_actividad::text
                    AND t.actespcod::text = e.cod_subactividad::text
                    AND t.servhoscod::text = e.cod_servicio::text

                    LEFT JOIN atenciones a
                        ON t.propertipoprogperscod = '1'
                    AND a.cod_oricentro     = t.oricenasicod
                    AND a.cod_centro        = t.cenasicod
                    AND a.cod_area          = t.arehoscod
                    AND a.cod_servicio      = t.servhoscod
                    AND a.cod_actividad     = t.actcod
                    AND a.cod_subactividad  = t.actespcod
                    AND a.cod_tipdoc_medico = t.tipdocidenpercod
                    AND a.cod_doc_medico    = t.perasisdocidennum
                    AND a.fecha_cita        = t.properfec
                    AND a.horaini           = t.properturhorini
                    AND a.horafin           = t.properturhorfin

                    WHERE
                        t.oricenasicod IN ('1','2','3','4','5','6','7')
                        AND pr.grupocupcod = '01'
                        AND t.arehoscod = '01'
                        AND t.actcod = '91'
                        AND t.estprogcitcod IN ('2','4')
                        AND t.actespcod <> '092'
                ),

                base2 AS (
                    SELECT
                        b.*,

                        CASE
                            WHEN b.estado_progcita = '4' THEN
                                CASE
                                    WHEN b.ate > 0 AND b.ate < 5 THEN '1'

                                    WHEN b.ate::numeric <= (b.hras_prog * 5)
                                        THEN floor(b.ate / 5.0)::int::varchar

                                    ELSE floor(b.hras_prog)::int::varchar
                                END
                            ELSE
                                floor(b.hras_prog)::int::varchar
                        END AS horas_efec

                    FROM base b
                )

                SELECT
                    *,
                    CASE
                        WHEN horas_efec::numeric > total_horas
                            THEN total_horas
                        ELSE horas_efec::numeric
                    END AS horas_efec_def
                FROM base2;
        """
        actualizacion = f"""UPDATE dwsge.fecha_act
                            SET fecha_act = {time}
                            WHERE id=3"""
        # Leer datos primero
        df = pd.read_sql_query(query, conn_src)

        if df.empty:
            print(f"⚠️ No se encontraron datos para {anio}-{mes_str}")
            continue

        df.columns = df.columns.str.lower()

        # Ahora recién truncamos la partición
        try:
            cur_dst = conn_dst.cursor()
            partition_name = f"dwsge.dwe_consulta_externa_horas_efectivas_{anio}_{mes_str}"
            cur_dst.execute(f"TRUNCATE TABLE {partition_name};")
            conn_dst.commit()
            cur_dst.close()
            print(f"Partición truncada correctamente: {partition_name}")
        except Exception as e:
            print(f"❌ Error truncando partición {partition_name}: {e}")
            continue

        # Cargar datos
        table_name = "dwsge.dwe_consulta_externa_horas_efectivas"
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        with conn_dst.cursor() as cur_dst:
            cols = ','.join(df.columns)
            copy_sql = f"COPY {table_name} ({cols}) FROM STDIN WITH CSV"
            cur_dst.copy_expert(sql=copy_sql, file=buffer)
            conn_dst.commit()
            cur_dst.execute(actualizacion)
            conn_dst.commit()

        print(f"✅ Carga completada para {anio}-{mes_str}: filas cargadas {len(df)}")

    except Exception as e:
        print(f"❌ Error en {anio}-{mes_str}: {e}")

# Cierre de conexiones
conn_src.close()
conn_dst.close()

end_time = datetime.now()
print(f"\n🕓 Proceso finalizado en {end_time - start_time}")
