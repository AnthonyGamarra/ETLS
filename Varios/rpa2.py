import os
import time
import random
import logging
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Configuración de Logs para monitorear el progreso en consola
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

load_dotenv()

# ============================================================
# CONFIGURACIÓN DE BASE DE DATOS Y URLS
# ============================================================
pg_user = os.getenv("PG_USER")
pg_pass = os.getenv("PG_PASS")
pg_host = os.getenv("PG_HOST")
pg_port = os.getenv("PG_PORT", "5433")
pg_db   = os.getenv("PG_DB")

URL_HOME     = "https://conoceatumedico.cmp.org.pe"
URL_INDEX    = "https://conoceatumedico.cmp.org.pe/index"

CAMPO_BUSQUEDA = "perasisprocolcod"
QUERY = f"""
    SELECT DISTINCT {CAMPO_BUSQUEDA}
    FROM dwsge.sgss_cmprs10
    WHERE perasisestregcod = '1'
      AND grupocupcod = '01'
      AND {CAMPO_BUSQUEDA} IS NOT NULL
"""

# ============================================================
# LECTURA DE BASE DE DATOS (PostgreSQL)
# ============================================================
def obtener_codigos():
    log.info(f"Conectando a PostgreSQL {pg_host}:{pg_port}/{pg_db}...")
    try:
        conn = psycopg2.connect(host=pg_host, port=pg_port, database=pg_db,
                                user=pg_user, password=pg_pass)
        df = pd.read_sql(QUERY, conn)
        conn.close()
        codigos = df[CAMPO_BUSQUEDA].dropna().astype(str).str.strip().tolist()
        log.info(f"[+] Se obtuvieron {len(codigos)} códigos CMP únicos para procesar.")
        return codigos
    except Exception as e:
        log.error(f"[-] Error al conectar o consultar la base de datos: {e}")
        return []

# ============================================================
# AUTOMATIZACIÓN DE NAVEGADOR (Selenium)
# ============================================================
def crear_driver_limpio():
    """Inicializa una instancia de Chrome limpia para eliminar tokens anteriores."""
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=options)
    driver.maximize_window()
    return driver

def solicitar_login_manual(driver):
    """Detiene el script para rotar la cuenta de Google antes del límite de 50 consultas."""
    driver.get(URL_HOME)
    while True:
        log.info("=" * 70)
        log.info("1. Inicia sesión en la ventana de Chrome con una cuenta de Google.")
        log.info("2. Cuando visualices el formulario de búsquedas, vuelve aquí.")
        log.info("3. Presiona ENTER en esta consola para iniciar el lote de consultas.")
        log.info("=" * 70)
        input()
        
        driver.get(URL_INDEX)
        time.sleep(2)
        inputs = driver.find_elements(By.CSS_SELECTOR, "input[placeholder*='12345'], input[placeholder*='Ejemplo']")
        if inputs and "index" in driver.current_url:
            log.info("[+] Sesión validada correctamente. Iniciando consultas en ráfaga...")
            return
        else:
            log.warning("[-] No se detectó el formulario de búsqueda. Intenta loguearte de nuevo.")
            driver.get(URL_HOME)

def extraer_medico(driver, wait, cmp_code):
    """Procesa el flujo visual en la web e interactúa con los componentes de React."""
    cmp_formateado = str(cmp_code).zfill(6)
    
    # Estructura base del registro para el Excel
    registro = {
        "cmp_buscado": cmp_formateado,
        "nombres": "NO ENCONTRADO",
        "especialidad": "MÉDICO GENERAL / SIN REGISTRO",
        "estado_web": "DESCONOCIDO",
        "status_proceso": "FALLIDO"
    }
    
    try:
        driver.get(URL_INDEX)
        time.sleep(0.5)

        # Ubicar campo de texto e ingresar CMP
        campo = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "input[placeholder*='12345'], input[placeholder*='Ejemplo']")
        ))
        campo.click()
        campo.clear()

        # Inyección forzada por JS para que React asimile el cambio de estado
        driver.execute_script("""
            var input = arguments[0];
            var setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
            setter.call(input, arguments[1]);
            input.dispatchEvent(new Event('input',  { bubbles: true }));
            input.dispatchEvent(new Event('change', { bubbles: true }));
        """, campo, cmp_formateado)
        time.sleep(0.3)

        # Clic en Buscar Médico
        wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(normalize-space(),'Buscar Médico') and not(@disabled)]")
        )).click()

        # Esperar la respuesta de enrutamiento
        WebDriverWait(driver, 8).until(
            lambda d: "BusquedaColegiado" in d.current_url or 
                      d.find_elements(By.XPATH, "//*[contains(normalize-space(),'0 médicos') or contains(normalize-space(),'no se encontr')]")
        )

        if "BusquedaColegiado" not in driver.current_url:
            registro["status_proceso"] = "SIN RESULTADOS EN WEB"
            return registro

        # Clic en el botón "Ver" para ir al perfil
        boton_ver = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//button[normalize-space()='Ver'] | //button[contains(normalize-space(),'Ver') and not(contains(.,'('))]")
        ))
        boton_ver.click()

        # Cargar detalles del perfil
        wait.until(EC.url_contains("Detallecolegiado"))
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "h2.text-foreground")))
        time.sleep(1.8) # Espera técnica fija para que carguen los datos desencriptados por React

        # Captura del Nombre Completo
        try:
            registro["nombres"] = driver.find_element(By.CSS_SELECTOR, "h2.text-foreground").text.strip()
        except Exception:
            pass

        # Captura de la especialidad usando herencia estructural XPath
        try:
            elemento_esp = driver.find_element(
                By.XPATH, "//*[contains(text(), 'Especialidad') or contains(text(), 'RNE')]/following::td"
            )
            esp_texto = elemento_esp.text.strip()
            if esp_texto:
                registro["especialidad"] = esp_texto
        except NoSuchElementException:
            pass

        registro["status_proceso"] = "COMPLETADO"
        return registro

    except Exception as e:
        log.debug(f"Error detallado en procesamiento del CMP {cmp_code}: {e}")
        registro["status_proceso"] = f"ERROR: {str(e)}"
        return registro

# ============================================================
# CONTROLADOR DEL FLUJO PRINCIPAL
# ============================================================
if __name__ == "__main__":
    codigos_totales = obtener_codigos()
    
    if not codigos_totales:
        log.error("[-] No hay códigos para procesar. Abortando script.")
        exit()
        
    indice_actual = 0
    total_codigos = len(codigos_totales)
    
    # Lista donde se acumularán todos los diccionarios de datos extraídos
    datos_para_excel = []
    
    while indice_actual < total_codigos:
        # Abrimos navegador e iniciamos sesión con la cuenta de turno
        driver = crear_driver_limpio()
        wait = WebDriverWait(driver, 10)
        
        solicitar_login_manual(driver)
        
        # Procesamos un lote de 47 consultas antes de que expire el token de la sesión activa
        consultas_lote = 0
        lote_maximo = min(47, total_codigos - indice_actual)
        
        log.info(f"== Iniciando procesamiento de lote ({lote_maximo} consultas disponibles) ==")
        
        for _ in range(lote_maximo):
            cmp_codigo = codigos_totales[indice_actual]
            
            # Consultar e interactuar con la interfaz gráfica
            resultado_medico = extraer_medico(driver, wait, cmp_codigo)
            
            # Almacenar en la lista en memoria
            datos_para_excel.append(resultado_medico)
            log.info(f"[{indice_actual + 1}/{total_codigos}] -> CMP: {cmp_codigo} | {resultado_medico['especialidad']}")
                
            indice_actual += 1
            consultas_lote += 1
            
            # Delay mínimo optimizado entre peticiones
            time.sleep(random.uniform(2.5, 3.8))
            
        # Fin del lote: cerramos navegador para purgar el Rate Limiter de la cuenta
        log.info(f"[!] Lote de {consultas_lote} consultas terminado. Cerrando ventana para prevenir bloqueo 401.")
        driver.quit()
        
        # Guardado preventivo en Excel por si decides detener el script a la mitad del proceso
        df_parcial = pd.DataFrame(datos_para_excel)
        df_parcial.to_excel("resultados_medicos.xlsx", index=False)
        log.info("[+] Archivo 'resultados_medicos.xlsx' actualizado en disco.")
        
        if indice_actual < total_codigos:
            log.info("[*] Preparando el siguiente bloque. Por favor, ten lista otra cuenta de Google para continuar.")
            time.sleep(3)

    log.info("\n[+] Proceso de extracción masiva completado exitosamente.")
    log.info(f"[+] Archivo final guardado con {len(datos_para_excel)} registros en: {os.path.abspath('resultados_medicos.xlsx')}")
