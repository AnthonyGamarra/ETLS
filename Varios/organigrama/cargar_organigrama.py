#!/usr/bin/env python3
"""
Carga el organigrama institucional en PostgreSQL (esquema dwsge) y valida el resultado.

Uso:
    pip install psycopg2-binary
    python cargar_organigrama.py                 # lee .env junto al script (o en la carpeta actual)
    python cargar_organigrama.py --env ruta\\.env
    python cargar_organigrama.py --solo-validar  # no ejecuta el SQL, solo corre las validaciones

Variables del .env:  PG_USER, PG_PASS, PG_HOST, PG_DB  (opcional: PG_PORT, por defecto 5432)

El script SQL es idempotente y no destructivo: crea lo que falta y no sobreescribe filas existentes.
"""
import argparse
import sys
from pathlib import Path

try:
    import psycopg2
except ImportError:
    sys.exit("Falta psycopg2. Instala con:  pip install psycopg2-binary")

AQUI = Path(__file__).resolve().parent
UNIDADES_ESPERADAS = 171
FUENTE = "ORGANIGRAMA_2016_18 Modificatoria"


def leer_env(ruta: Path) -> dict:
    datos = {}
    for linea in ruta.read_text(encoding="utf-8-sig").splitlines():
        linea = linea.strip()
        if not linea or linea.startswith("#") or "=" not in linea:
            continue
        clave, valor = linea.split("=", 1)
        valor = valor.strip()
        if len(valor) >= 2 and valor[0] == valor[-1] and valor[0] in "\"'":
            valor = valor[1:-1]
        datos[clave.strip()] = valor
    return datos


def buscar_env(arg_env):
    candidatos = [Path(arg_env)] if arg_env else [AQUI / ".env", Path.cwd() / ".env"]
    for c in candidatos:
        if c.is_file():
            return c
    sys.exit("No encontré el archivo .env (busqué en: %s)" % ", ".join(str(c) for c in candidatos))


def validar(cur) -> bool:
    ok = True

    def uno(sql, params=None):
        cur.execute(sql, params)
        return cur.fetchone()[0]

    def chequeo(etiqueta, valor, esperado):
        nonlocal ok
        bien = valor == esperado
        ok = ok and bien
        print("  [%s] %s: %s%s" % ("OK" if bien else "ERROR", etiqueta, valor,
                                    "" if bien else "  (esperado %s)" % esperado))

    print("\nValidaciones")
    chequeo("unidades cargadas (fuente %s)" % FUENTE,
            uno("SELECT count(*) FROM dwsge.organigrama_unidad WHERE fuente = %s", (FUENTE,)),
            UNIDADES_ESPERADAS)
    chequeo("raíces (unidades sin padre)",
            uno("SELECT count(*) FROM dwsge.organigrama_unidad WHERE id_unidad_padre IS NULL"), 1)
    chequeo("unidades inalcanzables desde la raíz (huérfanas o en ciclo)",
            uno("""SELECT count(*) FROM dwsge.organigrama_unidad u
                   WHERE NOT EXISTS (SELECT 1 FROM dwsge.vw_organigrama_jerarquia v WHERE v.id_unidad = u.id_unidad)"""), 0)
    chequeo("unidades con nivel guardado distinto al nivel recursivo",
            uno("""SELECT count(*) FROM dwsge.vw_organigrama_jerarquia v
                   JOIN dwsge.organigrama_unidad u USING (id_unidad) WHERE u.nivel <> v.nivel"""), 0)

    cur.execute("""SELECT tipo_organo, count(*) FROM dwsge.vw_organigrama_jerarquia
                   GROUP BY tipo_organo, 1 ORDER BY min(ruta_orden)""".replace("GROUP BY tipo_organo, 1", "GROUP BY tipo_organo"))
    print("\nUnidades por tipo de órgano")
    for nombre, n in cur.fetchall():
        print("  %-55s %4d" % (nombre, n))

    cur.execute("SELECT tipo_unidad, count(*) FROM dwsge.vw_organigrama_jerarquia GROUP BY tipo_unidad ORDER BY 2 DESC")
    print("\nUnidades por tipo de unidad")
    for nombre, n in cur.fetchall():
        print("  %-35s %4d" % (nombre, n))

    cur.execute("""SELECT ruta_jerarquica FROM dwsge.vw_organigrama_jerarquia
                   WHERE nombre = 'SUB GERENCIA DE ESTADÍSTICA'""")
    fila = cur.fetchone()
    print("\nPrueba de ruta recursiva (SUB GERENCIA DE ESTADÍSTICA):")
    print("  " + (fila[0] if fila else "no encontrada"))
    ok = ok and fila is not None

    n_val = uno("SELECT count(*) FROM dwsge.organigrama_unidad WHERE requiere_validacion")
    print("\nUnidades marcadas con requiere_validacion = true: %d" % n_val)
    print("  consulta:  SELECT codigo, nombre, nota FROM dwsge.organigrama_unidad "
          "WHERE requiere_validacion ORDER BY codigo;")
    return ok


def main():
    ap = argparse.ArgumentParser(description="Carga el organigrama en dwsge")
    ap.add_argument("--env", help="ruta al archivo .env")
    ap.add_argument("--sql", default=str(AQUI / "organigrama_dwsge.sql"), help="ruta al script SQL")
    ap.add_argument("--solo-validar", action="store_true", help="no ejecuta el SQL; solo valida")
    args = ap.parse_args()

    env = leer_env(buscar_env(args.env))
    faltan = [k for k in ("PG_USER", "PG_PASS", "PG_HOST", "PG_DB") if not env.get(k)]
    if faltan:
        sys.exit("Faltan variables en el .env: " + ", ".join(faltan))

    print("Conectando a %s:%s / %s como %s ..." % (env["PG_HOST"], env.get("PG_PORT", "5432"), env["PG_DB"], env["PG_USER"]))
    try:
        conn = psycopg2.connect(host=env["PG_HOST"], port=env.get("PG_PORT", "5432"), dbname=env["PG_DB"],
                                user=env["PG_USER"], password=env["PG_PASS"], connect_timeout=10)
    except psycopg2.Error as e:
        sys.exit("No se pudo conectar: %s" % str(e).strip())

    conn.autocommit = True      # el script SQL maneja su propia transacción (BEGIN ... COMMIT)
    with conn, conn.cursor() as cur:
        if not args.solo_validar:
            sql = Path(args.sql).read_text(encoding="utf-8")
            print("Ejecutando %s ..." % Path(args.sql).name)
            try:
                cur.execute(sql)
            except psycopg2.Error as e:
                cur.execute("ROLLBACK") if not conn.closed else None
                sys.exit("Error al ejecutar el script (se revirtió la transacción): %s" % str(e).strip())
            print("Script ejecutado.")
        ok = validar(cur)
    conn.close()
    print("\nResultado: %s" % ("TODO CORRECTO" if ok else "HAY VALIDACIONES CON ERROR"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
