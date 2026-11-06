-- Vista materializada dwsge.mv_data_ses
-- Data SES (dwsge.data_ses). El grupo FONAFE NO aplica a esta data:
-- está en dwsge.mv_data_ses_fonafe (ver mv_data_ses_fonafe.sql).
-- Refrescar: REFRESH MATERIALIZED VIEW dwsge.mv_data_ses;

BEGIN;

DROP MATERIALIZED VIEW IF EXISTS dwsge.mv_data_ses;

CREATE MATERIALIZED VIEW dwsge.mv_data_ses AS
SELECT familias,
    codpre,
    unidad_medida,
    codred_ses,
    red_ses,
    codcentro_ses,
    desccentro_ses,
    cod_nivel,
    mes,
    "año",
    cod_grupo,
    desc_grupo,
    cod_acti_ses,
    desc_act_ses,
    cod_actividad,
    descripcion_actividad,
    asegurado,
    no_asegurados,
    nivel_atencion
FROM dwsge.data_ses;



GRANT SELECT ON dwsge.mv_data_ses
    TO app_user, ggi, heraldlop, jhon, kettycord, oscarbell, percyquint, raulvald, raymundopill, rodrigofab;

COMMIT;
