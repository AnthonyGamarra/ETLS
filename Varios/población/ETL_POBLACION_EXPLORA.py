"""
ETL Población GCSPE -> dssge.poblacion_gcspe_sge

Lee todos los Excel de la carpeta poblacion_explora y los carga a PostgreSQL.

Por cada archivo:
  1. Valida que las columnas del Excel coincidan con las de la tabla destino
     (faltantes, sobrantes, duplicadas). Si no coinciden, el archivo NO se carga.
  2. Carga los datos a una tabla temporal y valida contenido
     (longitudes VARCHAR, cantidad numérica, periodo informado).
  3. Reemplaza en la tabla destino los periodos (poblacion_acumulada) que trae
     el archivo: DELETE de esos periodos + INSERT. Recargar un archivo no duplica.
  Cada archivo va en su propia transacción: si falla, se revierte solo ese archivo.

Uso:
  python ETL_POBLACION_EXPLORA.py              -> valida y carga
  python ETL_POBLACION_EXPLORA.py --validar    -> solo valida columnas, no carga
"""
import os
import re
import sys
import glob
import openpyxl
import pandas as pd
from io import StringIO
from dotenv import load_dotenv
from datetime import datetime
import psycopg2

# ==============================
# 1. Configuración
# ==============================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(SCRIPT_DIR, "..", ".env"))

pg_user = os.getenv("PG_USER")
pg_pass = os.getenv("PG_PASS")
pg_host = os.getenv("PG_HOST")
pg_port = os.getenv("PG_PORT", "5433")
pg_db   = os.getenv("PG_DB")

CARPETA_EXCEL = r"C:\Users\gcpp.ggi.sge2\Desktop\GGI\poblacion\poblacion_explora"
SCHEMA = "dssge"
TABLA = "poblacion_gcspe_sge"
TABLA_DESTINO = f"{SCHEMA}.{TABLA}"
COLUMNA_PERIODO = "poblacion_acumulada"
CHUNK_SIZE = 50000

SOLO_VALIDAR = "--validar" in sys.argv


# ==============================
# 2. Utilidades
# ==============================
def normalizar_columna(nombre):
    nombre = str(nombre or "").strip().lower()
    nombre = nombre.replace("-", "_").replace(" ", "_")
    nombre = re.sub(r"[^a-z0-9_]", "", nombre)
    return nombre


def obtener_estructura_tabla(cursor):
    cursor.execute("""
        SELECT column_name, data_type, character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (SCHEMA, TABLA))
    filas = cursor.fetchall()
    if not filas:
        raise RuntimeError(f"La tabla {TABLA_DESTINO} no existe o no tiene columnas.")
    return {nombre: (tipo, largo) for nombre, tipo, largo in filas}


def validar_columnas(columnas_excel, columnas_tabla):
    """Devuelve lista de errores; vacía si las columnas coinciden."""
    errores = []
    vacias = [i + 1 for i, c in enumerate(columnas_excel) if not c]
    if vacias:
        errores.append(f"Columnas sin nombre en las posiciones: {vacias}")

    nombradas = [c for c in columnas_excel if c]
    duplicadas = sorted({c for c in nombradas if nombradas.count(c) > 1})
    if duplicadas:
        errores.append(f"Columnas duplicadas en el Excel: {duplicadas}")

    faltantes = [c for c in columnas_tabla if c not in nombradas]
    sobrantes = [c for c in nombradas if c not in columnas_tabla]
    if faltantes:
        errores.append(f"Columnas de la tabla que NO están en el Excel: {faltantes}")
    if sobrantes:
        errores.append(f"Columnas del Excel que NO existen en la tabla: {sobrantes}")
    return errores


def limpiar_df(df, estructura):
    for col, (tipo, _) in estructura.items():
        if tipo in ("character varying", "character", "text"):
            serie = df[col].where(df[col].notna(), None)
            df[col] = serie.map(
                lambda v: None if v is None else str(v).replace("\x00", "").strip() or None
            )
    # "100 A MAS" -> "100"; el resto de valores ya vienen como el número puro.
    if "rango_edad" in df.columns:
        df["rango_edad"] = df["rango_edad"].str.extract(r"(\d+)", expand=False)
    if "cantidad_asegurados" in df.columns:
        df["cantidad_asegurados"] = pd.to_numeric(df["cantidad_asegurados"], errors="coerce").astype("Int64")
    return df


def validar_datos(df, df_original, estructura, fila_inicio):
    """Valida longitudes y numéricos de un lote. Devuelve lista de errores."""
    errores = []
    for col, (tipo, largo) in estructura.items():
        if largo:
            excede = df[col].dropna().str.len() > largo
            if excede.any():
                idx = excede[excede].index[0]
                errores.append(
                    f"'{col}' excede {largo} caracteres en {excede.sum()} fila(s); "
                    f"ej. fila Excel {fila_inicio + idx}: '{df.at[idx, col]}'"
                )
    if "cantidad_asegurados" in df.columns:
        invalidos = df["cantidad_asegurados"].isna() & df_original["cantidad_asegurados"].notna()
        if invalidos.any():
            idx = invalidos[invalidos].index[0]
            errores.append(
                f"'cantidad_asegurados' no numérico en {invalidos.sum()} fila(s); "
                f"ej. fila Excel {fila_inicio + idx}: '{df_original.at[idx, 'cantidad_asegurados']}'"
            )
    sin_periodo = df[COLUMNA_PERIODO].isna()
    if sin_periodo.any():
        errores.append(f"'{COLUMNA_PERIODO}' vacío en {sin_periodo.sum()} fila(s).")
    return errores


def copiar_a_postgres(cursor, df, table_name, columnas):
    csv_buffer = StringIO()
    df[columnas].to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)
    cursor.copy_expert(
        sql=f"COPY {table_name} ({', '.join(columnas)}) FROM STDIN WITH CSV",
        file=csv_buffer,
    )


def leer_cabecera(ruta):
    wb = openpyxl.load_workbook(ruta, read_only=True, data_only=True)
    ws = wb.worksheets[0]
    cabecera = next(ws.iter_rows(values_only=True, max_row=1), ())
    return wb, ws, list(cabecera)


# ==============================
# 3. Proceso por archivo
# ==============================
def procesar_archivo(cursor, conn, ruta, estructura):
    nombre = os.path.basename(ruta)
    columnas_tabla = list(estructura.keys())
    print(f"\n=== {nombre} ===")

    wb, ws, cabecera = leer_cabecera(ruta)
    try:
        columnas_excel = [normalizar_columna(c) for c in cabecera]
        print(f"Hoja leída: '{ws.title}' ({len(columnas_excel)} columnas)")

        errores = validar_columnas(columnas_excel, columnas_tabla)
        if errores:
            print("  [ERROR] Las columnas no coinciden con la tabla destino:")
            for e in errores:
                print(f"    - {e}")
            return "rechazado"

        if [c for c in columnas_excel if c] != columnas_tabla:
            print("  [INFO] Columnas en distinto orden que la tabla; se mapean por nombre.")
        print("  [OK] Columnas validadas.")

        if SOLO_VALIDAR:
            return "validado"

        # Carga a tabla temporal
        cursor.execute("DROP TABLE IF EXISTS tmp_poblacion;")
        cursor.execute(f"CREATE TEMP TABLE tmp_poblacion (LIKE {TABLA_DESTINO}) ON COMMIT DROP;")

        filas_iter = ws.iter_rows(min_row=2, values_only=True)
        total, num_lote, fila_excel = 0, 1, 2
        while True:
            lote = []
            for fila in filas_iter:
                if all(v is None for v in fila):
                    continue
                lote.append(fila)
                if len(lote) >= CHUNK_SIZE:
                    break
            if not lote:
                break

            df_original = pd.DataFrame(lote, columns=columnas_excel)
            df = limpiar_df(df_original.copy(), estructura)
            errores = validar_datos(df, df_original, estructura, fila_excel)
            if errores:
                print(f"  [ERROR] Datos inválidos en el lote {num_lote}:")
                for e in errores:
                    print(f"    - {e}")
                conn.rollback()
                return "rechazado"

            copiar_a_postgres(cursor, df, "tmp_poblacion", columnas_tabla)
            total += len(lote)
            fila_excel += len(lote)
            print(f"  Lote {num_lote}: {len(lote)} filas leídas (acumulado {total}).")
            num_lote += 1

        if total == 0:
            print("  [ERROR] El archivo no tiene filas de datos.")
            conn.rollback()
            return "rechazado"

        cursor.execute(f"SELECT DISTINCT {COLUMNA_PERIODO} FROM tmp_poblacion ORDER BY 1;")
        periodos = [r[0] for r in cursor.fetchall()]
        print(f"  Periodos en el archivo: {periodos}")

        cursor.execute(
            f"DELETE FROM {TABLA_DESTINO} WHERE {COLUMNA_PERIODO} IN (SELECT DISTINCT {COLUMNA_PERIODO} FROM tmp_poblacion);"
        )
        if cursor.rowcount:
            print(f"  Se reemplazan {cursor.rowcount} filas existentes de esos periodos.")

        cols = ", ".join(columnas_tabla)
        cursor.execute(f"INSERT INTO {TABLA_DESTINO} ({cols}) SELECT {cols} FROM tmp_poblacion;")
        insertadas = cursor.rowcount
        conn.commit()
        print(f"  [OK] {insertadas} filas cargadas en {TABLA_DESTINO}.")
        return "cargado"

    except Exception:
        conn.rollback()
        raise
    finally:
        wb.close()


# ==============================
# 4. Main
# ==============================
def main():
    archivos = sorted(
        f for f in glob.glob(os.path.join(CARPETA_EXCEL, "*.xls*"))
        if not os.path.basename(f).startswith("~$")
    )
    if not archivos:
        print(f"No hay archivos Excel en {CARPETA_EXCEL}")
        return

    print(f"Conectando a PostgreSQL en {pg_host}:{pg_port}, base de datos: {pg_db}...")
    conn = psycopg2.connect(host=pg_host, port=pg_port, dbname=pg_db, user=pg_user, password=pg_pass)
    conn.autocommit = False
    cursor = conn.cursor()

    start_time = datetime.now()
    print(f"Inicio del ETL: {start_time:%Y-%m-%d %H:%M:%S}  (modo: {'SOLO VALIDAR' if SOLO_VALIDAR else 'CARGA'})")
    print(f"Archivos encontrados: {len(archivos)}")

    resultados = {}
    try:
        estructura = obtener_estructura_tabla(cursor)
        conn.commit()
        for ruta in archivos:
            try:
                resultados[os.path.basename(ruta)] = procesar_archivo(cursor, conn, ruta, estructura)
            except Exception as e:
                print(f"  [ERROR] Falló el archivo, se revirtieron sus cambios: {e}")
                resultados[os.path.basename(ruta)] = "error"
    finally:
        cursor.close()
        conn.close()

    end_time = datetime.now()
    print("\n=== Resumen ===")
    for archivo, estado in resultados.items():
        print(f"  {estado.upper():10} {archivo}")
    print(f"Duración total: {end_time - start_time}")

    if any(e in ("rechazado", "error") for e in resultados.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
