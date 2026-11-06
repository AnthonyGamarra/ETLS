-- Data FONAFE: dbo.USP_FONAFE_GCOP_V3 -> dwsge.data_ses_fonafe (ETL_DW_DATA_SES_FONAFE.py)
-- Agrupación: agrupacion_fonafe.xlsx -> dwsge.agrupacion_fonafe (ETL_DIM_AGRUPACION_FONAFE.py)
-- Refrescar: REFRESH MATERIALIZED VIEW dwsge.mv_data_ses_fonafe;

BEGIN;

CREATE TABLE IF NOT EXISTS dwsge.data_ses_fonafe (
    codred_ses             integer,
    red_ses                varchar(200),
    codcentro_ses          integer,
    desccentro_ses         varchar(200),
    nivel                  varchar(50),
    mes                    varchar(20),
    "año"                  varchar(4),
    cod_actividad          integer,
    descripcion_actividad  varchar(200),
    asegurados             integer,
    no_asegurados          integer,
    nivel_atencion         varchar(50)
);
CREATE INDEX IF NOT EXISTS ix_data_ses_fonafe_periodo ON dwsge.data_ses_fonafe ("año", mes);

CREATE TABLE IF NOT EXISTS dwsge.agrupacion_fonafe (
    cod_actividad          integer PRIMARY KEY,
    descripcion_actividad  varchar(200),
    grupo_fonafe           varchar(100)
);

DROP MATERIALIZED VIEW IF EXISTS dwsge.mv_data_ses_fonafe;

-- grupo_fonafe queda NULL si aparece una actividad nueva que no está en el Excel.
CREATE MATERIALIZED VIEW dwsge.mv_data_ses_fonafe AS
SELECT a.codred_ses,
    a.red_ses,
    a.codcentro_ses,
    a.desccentro_ses,
    a.nivel,
    a.mes,
    a."año",
    a.cod_actividad,
    a.descripcion_actividad,
    g.grupo_fonafe,
    a.asegurados,
    a.no_asegurados,
    a.nivel_atencion
FROM dwsge.data_ses_fonafe a
LEFT JOIN dwsge.agrupacion_fonafe g
       ON g.cod_actividad = a.cod_actividad;

CREATE INDEX ix_mv_data_ses_fonafe_periodo ON dwsge.mv_data_ses_fonafe ("año", mes);
CREATE INDEX ix_mv_data_ses_fonafe_grupo_fonafe ON dwsge.mv_data_ses_fonafe (grupo_fonafe);

GRANT SELECT ON dwsge.mv_data_ses_fonafe
    TO app_user, ggi, heraldlop, jhon, kettycord, oscarbell, percyquint, raulvald, raymundopill, rodrigofab;

COMMIT;
