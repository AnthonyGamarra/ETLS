"""
Exporta el resultado de la query ATENCIONES_ONCO a un CSV por año (2019-2026).

Uso:
    python ETL_ATENCIONES_ONCO_EXPORT.py
    python ETL_ATENCIONES_ONCO_EXPORT.py --anios 2024 2025

Requisitos:
    pip install psycopg2-binary python-dotenv

Variables del .env:  PG_USER, PG_PASS, PG_HOST, PG_DB  (opcional: PG_PORT, por defecto 5433)
"""
import os
import re
import sys
import time
import argparse
import logging
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

AQUI = Path(__file__).resolve().parent

# ============================================================
# CONFIGURACIÓN
# ============================================================
QUERY_FILE = AQUI / "ATENDIDOS_ONCO"
OUTPUT_DIR = Path.home() / "Desktop" / "ATENDIDOS_ONCO"
ANIOS      = list(range(2019, 2027))

# Busca el .env subiendo desde la carpeta del script (Varios/.env)
for carpeta in [AQUI, *AQUI.parents]:
    if (carpeta / ".env").exists():
        load_dotenv(carpeta / ".env")
        break

pg_user = os.getenv("PG_USER")
pg_pass = os.getenv("PG_PASS")
pg_host = os.getenv("PG_HOST")
pg_port = os.getenv("PG_PORT", "5433")
pg_db   = os.getenv("PG_DB")

# Filtro de año en la query original: WHERE ANIO IN ('2019')
PATRON_ANIO = re.compile(r"WHERE\s+ANIO\s+IN\s*\([^)]*\)", re.IGNORECASE)


def leer_query():
    sql = QUERY_FILE.read_text(encoding="utf-8").strip().rstrip(";")
    if not PATRON_ANIO.search(sql):
        sys.exit("No se encontró el filtro \"WHERE ANIO IN (...)\" en " + str(QUERY_FILE))
    return sql


def query_por_anio(sql, anio):
    return PATRON_ANIO.sub(f"WHERE ce.anio IN ('{anio}')", sql, count=1)


def exportar_anio(conn, sql, anio):
    destino = OUTPUT_DIR / f"atendidos_onco_{anio}.csv"
    copy_sql = f"COPY ({query_por_anio(sql, anio)}) TO STDOUT WITH (FORMAT CSV, HEADER, DELIMITER ',')"

    log.info(f"Exportando {anio} → {destino.name} ...")
    t0 = time.time()
    # utf-8-sig para que Excel lea bien las tildes y ñ
    with open(destino, "w", encoding="utf-8-sig", newline="") as f, conn.cursor() as cur:
        cur.copy_expert(copy_sql, f)
    with open(destino, "r", encoding="utf-8-sig") as f:
        filas = sum(1 for _ in f) - 1
    log.info(f"  → {filas:,} filas en {time.time() - t0:.1f}s")
    return filas


def main():
    ap = argparse.ArgumentParser(description="Exporta ATENCIONES_ONCO a CSV por año")
    ap.add_argument("--anios", nargs="+", type=int, default=ANIOS, help="años a exportar")
    args = ap.parse_args()

    faltan = [k for k, v in {"PG_USER": pg_user, "PG_PASS": pg_pass,
                             "PG_HOST": pg_host, "PG_DB": pg_db}.items() if not v]
    if faltan:
        sys.exit("Faltan variables en el .env: " + ", ".join(faltan))

    sql = leer_query()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    log.info(f"Conectando a PostgreSQL {pg_host}:{pg_port}/{pg_db}...")
    conn = psycopg2.connect(host=pg_host, port=pg_port, database=pg_db,
                            user=pg_user, password=pg_pass, connect_timeout=10)
    resumen = {}
    try:
        for anio in args.anios:
            try:
                resumen[anio] = exportar_anio(conn, sql, anio)
            except psycopg2.Error as e:
                conn.rollback()
                log.error(f"  Error en {anio}: {str(e).strip()}")
                resumen[anio] = None
    finally:
        conn.close()

    log.info("=" * 50)
    for anio, filas in resumen.items():
        log.info(f"  {anio}: {'ERROR' if filas is None else f'{filas:,} filas'}")
    log.info(f"CSVs guardados en: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
