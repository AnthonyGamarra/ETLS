import os
import psycopg2
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from dotenv import load_dotenv
from datetime import datetime
import time
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

cmp_email    = os.getenv("CMP_EMAIL")
cmp_password = os.getenv("CMP_PASSWORD")

URL_HOME     = "https://conoceatumedico.cmp.org.pe"
URL_INDEX    = "https://conoceatumedico.cmp.org.pe/index"       # pantalla de búsqueda (post-login)
URL_BUSQUEDA = "https://conoceatumedico.cmp.org.pe/BusquedaColegiado"  # resultados
URL_DETALLE  = "https://conoceatumedico.cmp.org.pe/Detallecolegiado"   # detalle del médico

CAMPO_BUSQUEDA = "perasisprocolcod"

QUERY = f"""
    SELECT DISTINCT {CAMPO_BUSQUEDA}
    FROM dwsge.sgss_cmprs10
    WHERE perasisestregcod = '1'
      AND grupocupcod = '01'
      AND {CAMPO_BUSQUEDA} IS NOT NULL
    limit 20
"""

# La web impone ~12 s de cooldown entre consultas; usamos 16 s por seguridad
COOLDOWN = 18.0
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

# ============================================================
# BASE DE DATOS
# ============================================================
def obtener_codigos():
    log.info(f"Conectando a PostgreSQL {pg_host}:{pg_port}/{pg_db}...")
    conn = psycopg2.connect(host=pg_host, port=pg_port, database=pg_db,
                            user=pg_user, password=pg_pass)
    df = pd.read_sql(QUERY, conn)
    conn.close()
    codigos = df[CAMPO_BUSQUEDA].dropna().astype(str).str.strip().tolist()
    log.info(f"{len(codigos)} códigos obtenidos.")
    return codigos

# ============================================================
# SELENIUM
# ============================================================
def crear_driver(headless: bool = False):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=options)
    driver.maximize_window()
    return driver


def texto(el):
    try:
        return el.text.strip()
    except Exception:
        return ""


def esperar_login(driver):
    """Abre la web, espera login manual y verifica que la sesión sea válida."""
    driver.get(URL_HOME)

    while True:
        log.info("=" * 60)
        log.info("Iniciá sesión en el navegador con tu cuenta de Google.")
        log.info("Cuando veas el formulario 'Busca información de médicos colegiados',")
        log.info("volvé acá y apretá ENTER para continuar.")
        log.info("=" * 60)
        input()

        # Verificar sesión: navegamos a /index y comprobamos que el formulario esté visible
        log.info("Verificando sesión...")
        driver.get(URL_INDEX)
        time.sleep(2)

        # Si el input de búsqueda aparece → sesión válida
        inputs = driver.find_elements(By.CSS_SELECTOR, "input[placeholder*='12345'], input[placeholder*='Ejemplo']")
        if inputs and "index" in driver.current_url:
            log.info("Sesión verificada. Comenzando búsquedas.")
            return
        else:
            log.warning(f"Sesión no válida (URL: {driver.current_url}). Intentá loguearte de nuevo.")
            driver.get(URL_HOME)


def screenshot(driver, nombre: str):
    """Guarda una captura de pantalla para debugging."""
    ruta = os.path.join(OUTPUT_DIR, f"debug_{nombre}.png")
    driver.save_screenshot(ruta)
    log.info(f"Screenshot guardado: {ruta}")


def buscar_medico(driver, wait, cmp_code: str) -> dict:
    resultado = {
        "cmp_buscado":      cmp_code,
        "cmp_real":         "",
        "nombre":           "",
        "estado":           "",
        "consejo_regional": "",
        "error":            "",
    }

    try:
        # ── 1. Ir a /index (formulario de búsqueda) ────────────────────────
        driver.get(URL_INDEX)
        time.sleep(1)

        # El tab "Buscar por CMP" ya viene activo por defecto.
        # Si hubiera que clickearlo, buscar el botón que contiene "Buscar por CMP".
        try:
            driver.find_element(
                By.XPATH, "//button[contains(normalize-space(),'Buscar por CMP')]"
            ).click()
            time.sleep(0.5)
        except NoSuchElementException:
            pass  # ya está activo

        # ── 2. Ingresar CMP ────────────────────────────────────────────────
        # Input con placeholder "Ejemplo: 12345"
        campo = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "input[placeholder*='12345'], input[placeholder*='Ejemplo']")
        ))
        campo.click()
        campo.clear()

        # React no detecta send_keys como un cambio de estado; usamos JS para disparar los eventos
        driver.execute_script("""
            var input = arguments[0];
            var setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
            setter.call(input, arguments[1]);
            input.dispatchEvent(new Event('input',  { bubbles: true }));
            input.dispatchEvent(new Event('change', { bubbles: true }));
        """, campo, str(cmp_code))
        time.sleep(0.5)  # pequeña pausa para que React procese el estado

        # ── 3. Click en "Buscar Médico" (esperar que se habilite) ──────────
        # El botón empieza disabled="" y se habilita cuando el input tiene valor
        wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(normalize-space(),'Buscar Médico') and not(@disabled)]")
        )).click()

        # ── 4. Esperar resultados ──────────────────────────────────────────
        # Esperar a que el sitio navegue a /BusquedaColegiado (hay resultado)
        # o muestre un mensaje de sin resultado en /index
        WebDriverWait(driver, 20).until(
            lambda d: "BusquedaColegiado" in d.current_url
                      or d.find_elements(By.XPATH,
                         "//*[contains(normalize-space(),'0 médicos') "
                         "or contains(normalize-space(),'no se encontr') "
                         "or contains(normalize-space(),'sin resultado')]")
        )

        if "BusquedaColegiado" not in driver.current_url:
            resultado["error"] = "Sin resultado"
            return resultado

        # Verificar que la tabla tenga filas
        filas_resultado = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        if not filas_resultado:
            resultado["error"] = "Sin resultado"
            return resultado

        # Botón "Ver" — sin countdown porque el usuario está logueado
        boton_ver = wait.until(EC.element_to_be_clickable(
            (By.XPATH,
             "//button[normalize-space()='Ver']"
             " | //button[contains(normalize-space(),'Ver') and not(contains(.,'('))]")
        ))
        boton_ver.click()

        # ── 5. Página de detalle (/Detallecolegiado) ───────────────────────
        wait.until(EC.url_contains("Detallecolegiado"))
        # Esperar a que React termine de renderizar toda la página:
        # 1) el h2 con el nombre aparece (primera fase de render)
        wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "h2.text-foreground")
        ))
        # 2) pausa fija de 2 s para que el resto de secciones (estado, registros) terminen de pintarse
        time.sleep(2)

        # CMP real — heading que contiene "CMP:"
        try:
            cmp_el = driver.find_element(
                By.XPATH, "//*[contains(normalize-space(),'CMP:') and (self::h1 or self::h2 or self::h3 or self::h4)]"
            )
            resultado["cmp_real"] = cmp_el.text.replace("CMP:", "").strip()
        except NoSuchElementException:
            pass

        # Nombre — <h2 class="... text-foreground ...">
        try:
            nombre_el = driver.find_element(By.CSS_SELECTOR, "h2.text-foreground")
            resultado["nombre"] = texto(nombre_el)
        except NoSuchElementException:
            pass

        # Estado — <span> con clase bg-green-100 (HÁBIL) o bg-red-100 (INHÁBIL)
        try:
            estado_el = driver.find_element(
                By.CSS_SELECTOR, "span.bg-green-100, span.bg-red-100"
            )
            resultado["estado"] = texto(estado_el)
        except NoSuchElementException:
            pass

        # Consejo Regional — <p class="... text-secondary ..."> que empieza con "CONSEJO REGIONAL"
        try:
            consejo_el = driver.find_element(
                By.XPATH, "//p[contains(@class,'text-secondary') and starts-with(normalize-space(),'CONSEJO REGIONAL')]"
            )
            resultado["consejo_regional"] = texto(consejo_el)
        except NoSuchElementException:
            pass

        # ── 6. Sección "Registro Nacional de Estudios" ────────────────────
        filas_reg = driver.find_elements(
            By.XPATH, "//label[normalize-space()='Registro']/parent::div/parent::div"
        )
        for i, fila in enumerate(filas_reg, 1):
            def celda(label_txt):
                try:
                    return fila.find_element(
                        By.XPATH,
                        f".//label[normalize-space()='{label_txt}']/following-sibling::p[1]"
                    ).text.strip()
                except NoSuchElementException:
                    return ""
            resultado[f"registro_{i}"] = celda("Registro")
            resultado[f"tipo_{i}"]     = celda("Tipo")
            resultado[f"codigo_{i}"]   = celda("Código")
            resultado[f"fecha_{i}"]    = celda("Fecha")

    except TimeoutException:
        log.warning(f"Timeout con CMP: {cmp_code} — URL actual: {driver.current_url}")
        screenshot(driver, f"timeout_{cmp_code}")
        resultado["error"] = "Timeout"
    except Exception as e:
        log.error(f"Error con {cmp_code}: {e} — URL actual: {driver.current_url}")
        screenshot(driver, f"error_{cmp_code}")
        resultado["error"] = str(e)

    return resultado

# ============================================================
# PRINCIPAL
# ============================================================
def main():
    codigos = obtener_codigos()
    if not codigos:
        log.warning("No hay códigos para procesar. Revisa la query o los filtros.")
        return

    driver = crear_driver(headless=False)  # Google OAuth no funciona en headless
    wait   = WebDriverWait(driver, 25)
    resultados = []

    try:
        esperar_login(driver)

        for i, codigo in enumerate(codigos, 1):
            log.info(f"[{i}/{len(codigos)}] CMP: {codigo}")
            resultado = buscar_medico(driver, wait, codigo)
            resultados.append(resultado)
            log.info(
                f"  → {resultado.get('nombre') or '(sin nombre)'} "
                f"| {resultado.get('estado') or '?'} "
                f"| error: {resultado.get('error') or 'ninguno'}"
            )
            time.sleep(COOLDOWN)
    finally:
        driver.quit()

    df = pd.DataFrame(resultados)

    # Ordenar columnas: fijas primero, luego dinámicas agrupadas por prefijo:
    # registro_1, registro_2, ..., tipo_1, tipo_2, ..., codigo_1, ..., fecha_1, ...
    cols_fijas = ["cmp_buscado", "cmp_real", "nombre", "estado", "consejo_regional", "error"]
    orden_prefijo = ["registro_", "tipo_", "codigo_", "fecha_"]

    def sort_key(col):
        num = int("".join(c for c in col if c.isdigit()) or 0)
        prefijo = next((p for p in orden_prefijo if col.startswith(p)), col)
        pos = orden_prefijo.index(prefijo) if prefijo in orden_prefijo else 99
        return (pos, num)

    cols_dyn = sorted([c for c in df.columns if c not in cols_fijas], key=sort_key)
    df = df[cols_fijas + cols_dyn]

    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(OUTPUT_DIR, f"colegiatura_{timestamp}.xlsx")

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Resultados")
        ws = writer.sheets["Resultados"]
        for col in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 60)

    log.info(f"Exportado: {output_path}")
    log.info(
        f"Total: {len(df)} | OK: {(df['error'] == '').sum()} | Error: {(df['error'] != '').sum()}"
    )


if __name__ == "__main__":
    main()
