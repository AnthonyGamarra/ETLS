from Varios.PIAPOI.carga_utils import leer_excel, cargar_tabla

# ==============================
# Maestro de fondos (FONDOS.XLSX) -> dwsge.pre_fondos
# ==============================
df = leer_excel("FONDOS.XLSX", {
    "Fondo": "fondo",
    "Denominación": "denominacion",
    "Descripción": "descripcion",
    "Denominación.1": "denominacion_2",
})

cargar_tabla(df, "pre_fondos", """
    fondo           varchar(20) PRIMARY KEY,
    denominacion    varchar(200),
    descripcion     varchar(500),
    denominacion_2  varchar(200)
""")
