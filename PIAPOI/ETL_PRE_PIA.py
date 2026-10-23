from carga_utils import leer_excel, cargar_tabla

# ==============================
# PIA institucional (PIA 2026 INSTITUCIONAL.xlsx) -> dwsge.pre_pia
# ==============================
anio = 2026

df = leer_excel(f"PIA {anio} INSTITUCIONAL.xlsx", {
    "CAT3": "cat3",
    "CAT2": "cat2",
    "FONDO": "fondo",
    "CATEGORIA": "categoria",
    "RUBRO": "rubro",
    "COD": "posicion_presupuestaria",
    "CONCEPTO": "concepto",
    "DEPENDENCIA": "dependencia",
    "FONDO2": "fondo2",
    "Presupuesto": "presupuesto",
}, numericas=["presupuesto"], hoja=str(anio))
df.insert(0, "anio", anio)

cargar_tabla(df, "pre_pia", """
    anio                     integer,
    cat3                     varchar(50),
    cat2                     varchar(100),
    fondo                    varchar(50),
    categoria                varchar(50),
    rubro                    varchar(100),
    posicion_presupuestaria  varchar(20),
    concepto                 varchar(500),
    dependencia              varchar(200),
    fondo2                   varchar(200),
    presupuesto              numeric(18,2)
""")
