import os
import runpy
from datetime import datetime
from Varios.PIAPOI.carga_utils import conectar_pg

# ==============================
# Orquestador PIA / POI:
#   1. Carga los 5 Excel de presupuesto a dwsge
#   2. Crea (primera vez) o refresca la vista materializada dwsge.mv_pre_pia_ejecucion
# ==============================
CARPETA = os.path.dirname(os.path.abspath(__file__))
ETLS = [
    "ETL_PRE_FONDOS.py",
    "ETL_PRE_POSICIONES.py",
    "ETL_PRE_REDES.py",
    "ETL_PRE_PIA.py",
    "ETL_PRE_EJECUCION.py",
]

start_time = datetime.now()
print(f"\nInicio del ETL PIA/POI: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

for etl in ETLS:
    runpy.run_path(os.path.join(CARPETA, etl), run_name="__main__")

conn = conectar_pg()
try:
    with conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('dwsge.mv_pre_pia_ejecucion')")
        if cur.fetchone()[0] is None:
            with open(os.path.join(CARPETA, "mv_pre_pia_ejecucion.sql"), encoding="utf-8") as f:
                cur.execute(f.read())
            print("dwsge.mv_pre_pia_ejecucion: creada")
        else:
            cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY dwsge.mv_pre_pia_ejecucion")
            print("dwsge.mv_pre_pia_ejecucion: refrescada")
        cur.execute("SELECT count(*), sum(pia), sum(pres_modificado), sum(pres_ejecutado) FROM dwsge.mv_pre_pia_ejecucion")
        filas, pia, pim, eje = cur.fetchone()
        print(f"  filas={filas}  PIA={pia:,.0f}  PIM={pim:,.0f}  Ejecutado={eje:,.0f}")
finally:
    conn.close()

print(f"Fin del ETL PIA/POI. Duración: {datetime.now() - start_time}")
