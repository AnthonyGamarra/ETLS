-- Vista materializada dwsge.poblacion_asegurada_gcpp
-- Solo muestra el último periodo (poblacion_acumulada) cargado en dssge.poblacion_gcspe_sge.
-- Al ser materializada, toma el último mes en cada REFRESH:
--   REFRESH MATERIALIZED VIEW dwsge.poblacion_asegurada_gcpp;

BEGIN;

DROP MATERIALIZED VIEW IF EXISTS dwsge.poblacion_asegurada_gcpp;

CREATE MATERIALIZED VIEW dwsge.poblacion_asegurada_gcpp AS
SELECT a.poblacion_acumulada,
    a.ubigeo_inei,
    a.departamento,
    a.provincia,
    a.distrito,
    (b.tipo_sge::text || ' ' || b.sge1_denom_corta_sge::text) AS nombre_centro,
    b.tipo_sge1 AS tipo_centro_asistencial,
    b.cod_centro_essi_gcspe,
    b.nombre_red_gcspe AS nombre_red,
    a.condicion,
    a.tipo_atencion,
    a.sexo,
    CASE
        WHEN a.rango_edad::integer >= 75 THEN 'Mayor a 75'
        WHEN a.rango_edad::integer >= 70 THEN '70 - < 75'
        WHEN a.rango_edad::integer >= 66 THEN '66 - < 70'
        WHEN a.rango_edad::integer >= 65 THEN '65 - < 66'
        WHEN a.rango_edad::integer >= 60 THEN '60 - < 65'
        WHEN a.rango_edad::integer >= 50 THEN '50 - < 60'
        WHEN a.rango_edad::integer >= 40 THEN '40 - < 50'
        WHEN a.rango_edad::integer >= 30 THEN '30 - < 40'
        WHEN a.rango_edad::integer >= 20 THEN '20 - < 30'
        WHEN a.rango_edad::integer >= 18 THEN '18 - < 20'
        WHEN a.rango_edad::integer >= 15 THEN '15 - < 18'
        WHEN a.rango_edad::integer >= 12 THEN '12 - < 15'
        WHEN a.rango_edad::integer >= 11 THEN '11 - < 12'
        WHEN a.rango_edad::integer >= 10 THEN '10 - < 11'
        WHEN a.rango_edad::integer >= 5  THEN '05 - < 10'
        WHEN a.rango_edad::integer >= 4  THEN '04 - < 05'
        WHEN a.rango_edad::integer >= 3  THEN '03 - < 04'
        WHEN a.rango_edad::integer >= 2  THEN '02 - < 03'
        WHEN a.rango_edad::integer >= 1  THEN '01 - < 02'
        ELSE '<1'
    END AS rango_edad,
    sum(a.cantidad_asegurados) AS cantidad_asegurados
FROM dssge.poblacion_gcspe_sge a
LEFT JOIN dssge.poblacion_gcspe_sge_homologacion b
       ON b.cod_centro_essi_gcspe::text = a.cod_centro::text
-- Último mes cargado (formato YYYYMM, por eso MAX sobre texto ordena bien)
WHERE a.poblacion_acumulada = (SELECT MAX(poblacion_acumulada) FROM dssge.poblacion_gcspe_sge)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13;

-- Permisos de lectura que tenía la vista original (se pierden con el DROP)
GRANT SELECT ON dwsge.poblacion_asegurada_gcpp
    TO ggi, raulvald, percyquint, heraldlop, kettycord, rodrigofab, raymundopill, oscarbell, jhon, app_user;

COMMIT;
