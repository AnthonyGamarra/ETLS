import glob
import os
import pandas as pd
from Varios.PIAPOI.carga_utils import RUTA_PRESUPUESTO, leer_excel, cargar_tabla

# ==============================
# Maestro de centros gestores SAP (CENTROS GESTORES*.xls, hoja Hoja1) -> dwsge.pre_redes
# Columnas (por posición): centro gestor | descripción | red
# La red se normaliza a los nombres que usa la vista para cruzar con el PIA.
# ==============================

# Toma el archivo más reciente (el nombre puede traer sufijos como " (1)")
archivo = max(glob.glob(os.path.join(RUTA_PRESUPUESTO, "CENTROS GESTORES*.xls*")), key=os.path.getmtime)
print(f"Maestro de centros gestores: {os.path.basename(archivo)}")

# Se lee por posición porque los encabezados del Excel cambian
encabezados = pd.read_excel(archivo, sheet_name="Hoja1", nrows=0).columns[:3]
df = leer_excel(os.path.basename(archivo),
                dict(zip(encabezados, ["centro_gestor", "eess", "red"])), hoja="Hoja1")

df["red"] = df["red"].str.upper().replace({"CNSR": "C.N. SALUD RENAL"})
# La sede central se separa por fondo, igual que el PIA (AFESSALUD = 11..., SALUD = 12...)
sede = df["red"] == "SEDE CENTRAL"
df.loc[sede, "red"] = df.loc[sede, "centro_gestor"].str[:2].map({"11": "SCENTRALAfe"}).fillna("SCENTRALSal")

sin_red = df[df["red"].isna()]
if len(sin_red):
    print(f"  {len(sin_red)} centros gestores sin red:")
    print(sin_red[["centro_gestor", "eess"]].to_string(index=False))

cargar_tabla(df, "pre_redes", """
    centro_gestor  varchar(20) PRIMARY KEY,
    eess           varchar(200),
    red            varchar(100)
""")
