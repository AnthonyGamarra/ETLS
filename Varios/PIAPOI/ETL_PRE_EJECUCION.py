from Varios.PIAPOI.carga_utils import leer_excel, cargar_tabla

# ==============================
# Ejecución presupuestal SAP (ejecucion.xls) -> dwsge.pre_ejecucion
# (el archivo es un acumulado a la fecha: cada carga reemplaza la anterior)
# ==============================
montos = [
    "pia", "pres_modificado", "pres_asignable", "certificacion", "reserva", "solped",
    "pedidos", "c_acept_entreg", "pres_ejecutado", "pres_comprometido", "pres_asignado",
    "saldo_sin_asignar", "pct_avance_asignable", "pct_avance_pres_modificado", "pct_avance_pia",
]

df = leer_excel("ejecucion.xls", dict(zip([
    "Fondo", "Centro Gestor", "Pos. Presupuestaria", "Área Funcional",
    "PIA", "Pres. Modificado", "Pres. Asignable", "Certificación", "Reserva", "Solped",
    "Pedidos", "C.Acept/Entreg.", "Pres. Ejecutado", "Pres. comprometido", "Pres.Asignado",
    "Saldo Sin Asignar", "% Avance Asignable", "% Avance Pres. Modificado", "% Avance PIA",
], ["fondo", "centro_gestor", "posicion_presupuestaria", "area_funcional"] + montos)),
    numericas=montos)

cargar_tabla(df, "pre_ejecucion", """
    fondo                       varchar(20),
    centro_gestor               varchar(20),
    posicion_presupuestaria     varchar(20),
    area_funcional              varchar(50),
""" + ",\n".join(f"    {c} numeric" for c in montos))
