from carga_utils import leer_excel, cargar_tabla

# ==============================
# Maestro de posiciones presupuestarias (POSC.PRESPT.XLSX) -> dwsge.pre_posiciones
# ==============================
df = leer_excel("POSC.PRESPT.XLSX", {
    "Posición presupuestaria": "posicion_presupuestaria",
    "Denominación": "denominacion",
    "Descripción": "descripcion",
    "Denominación.1": "denominacion_2",
})

cargar_tabla(df, "pre_posiciones", """
    posicion_presupuestaria  varchar(20) PRIMARY KEY,
    denominacion             varchar(200),
    descripcion              varchar(500),
    denominacion_2           varchar(200)
""")
