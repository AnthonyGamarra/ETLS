import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

load_dotenv()

# ============================================================
# CONFIGURACIÓN
# ============================================================
pg_user = os.getenv("PG_USER")
pg_pass = os.getenv("PG_PASS")
pg_host = os.getenv("PG_HOST")
pg_port = os.getenv("PG_PORT", "5433")
pg_db   = os.getenv("PG_DB")

TABLAS = {
    "data_gctic_ce":  "SELECT * FROM dwsge.data_gctic_ce",
    "data_gctic_eme": "SELECT * FROM dwsge.data_gctic_eme",
}

OUTPUT_DIR = r"C:\Users\gcpp.ggi.sge2\Desktop\data_gctic"


def obtener_tablas():
    log.info(f"Conectando a PostgreSQL {pg_host}:{pg_port}/{pg_db}...")
    conn = psycopg2.connect(host=pg_host, port=pg_port, database=pg_db,
                             user=pg_user, password=pg_pass)
    dataframes = {}
    try:
        for nombre, query in TABLAS.items():
            log.info(f"Extrayendo {nombre}...")
            dataframes[nombre] = pd.read_sql(query, conn)
            log.info(f"  → {len(dataframes[nombre])} filas obtenidas")
    finally:
        conn.close()
    return dataframes


def exportar_excel(dataframes):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(OUTPUT_DIR, f"data_gctic_{timestamp}.xlsx")

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for nombre, df in dataframes.items():
            df.to_excel(writer, index=False, sheet_name=nombre[:31])
            ws = writer.sheets[nombre[:31]]
            for col in ws.columns:
                max_len = max(len(str(cell.value or "")) for cell in col)
                ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 60)

    log.info(f"Exportado: {output_path}")


def main():
    dataframes = obtener_tablas()
    exportar_excel(dataframes)


if __name__ == "__main__":
    main()
