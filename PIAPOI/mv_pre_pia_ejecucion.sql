-- =====================================================================
-- dwsge.mv_pre_pia_ejecucion
-- PIA vs. presupuesto modificado vs. ejecución, a nivel RED x POSICIÓN.
--
-- Relación entre tablas:
--   pre_ejecucion.centro_gestor           -> pre_redes.centro_gestor  (red)
--   pre_ejecucion.fondo                   -> pre_fondos.fondo         (n° de fondos)
--   pre_pia.dependencia / fondo           -> red  (ver CASE abajo)
--   posicion_presupuestaria (ambas)       -> pre_posiciones
--
-- El PIA de SEDE CENTRAL se asigna a la "red" del centro gestor de sede:
--   FONDO SALUD -> SCENTRALSal (12099A0000), AFESSALUD -> SCENTRALAfe (11099A0000).
-- Centros gestores de ejecución que no están en el maestro toman la red por su prefijo
-- de 5 caracteres; si tampoco hay prefijo conocido quedan como 'SIN RED'.
-- Filas del PIA sin código de posición (gastos de capital) usan FONDO2 como clave.
-- =====================================================================
DROP MATERIALIZED VIEW IF EXISTS dwsge.mv_pre_pia_ejecucion;

CREATE MATERIALIZED VIEW dwsge.mv_pre_pia_ejecucion AS
WITH pia AS (
    SELECT
        CASE
            WHEN p.categoria = 'SEDE CENTRAL' AND p.fondo = 'AFESSALUD' THEN 'SCENTRALAfe'
            WHEN p.categoria = 'SEDE CENTRAL'                          THEN 'SCENTRALSal'
            WHEN p.dependencia = 'CNSRENAL'                            THEN 'C.N. SALUD RENAL'
            WHEN p.dependencia = 'MA DE DIOS'                          THEN 'MADRE DE DIOS'
            ELSE p.dependencia
        END                                                   AS red,
        COALESCE(p.posicion_presupuestaria, p.fondo2)         AS posicion_presupuestaria,
        MIN(p.categoria)                                      AS categoria,
        MIN(p.rubro)                                          AS rubro,
        MIN(p.cat2)                                           AS tipo_gasto,
        MIN(p.concepto)                                       AS concepto_pia,
        SUM(p.presupuesto)                                    AS pia
    FROM dwsge.pre_pia p
    GROUP BY 1, 2
),
red_prefijo AS (   -- respaldo: red por prefijo de 5 caracteres para CG que no están en el maestro
    SELECT DISTINCT left(centro_gestor, 5) AS prefijo, red
    FROM dwsge.pre_redes
    WHERE red IS NOT NULL
),
eje AS (
    SELECT
        COALESCE(r.red, rp.red, 'SIN RED') AS red,
        e.posicion_presupuestaria,
        COUNT(DISTINCT e.centro_gestor)    AS n_centros_gestores,
        COUNT(DISTINCT e.fondo)            AS n_fondos,
        STRING_AGG(DISTINCT f.denominacion, ' | ' ORDER BY f.denominacion) AS fondos,
        SUM(e.pres_modificado)             AS pres_modificado,
        SUM(e.pres_asignable)              AS pres_asignable,
        SUM(e.certificacion)               AS certificacion,
        SUM(e.pres_comprometido)           AS pres_comprometido,
        SUM(e.pres_ejecutado)              AS pres_ejecutado,
        SUM(e.saldo_sin_asignar)           AS saldo_sin_asignar
    FROM dwsge.pre_ejecucion e
    LEFT JOIN dwsge.pre_redes  r ON r.centro_gestor = e.centro_gestor
    LEFT JOIN red_prefijo     rp ON rp.prefijo      = left(e.centro_gestor, 5)
    LEFT JOIN dwsge.pre_fondos f ON f.fondo         = e.fondo
    GROUP BY 1, 2
),
rubro_pos AS (   -- rubro/tipo de gasto por posición, para filas que solo existen en ejecución
    SELECT DISTINCT ON (posicion_presupuestaria)
        posicion_presupuestaria, rubro, cat2
    FROM dwsge.pre_pia
    WHERE posicion_presupuestaria IS NOT NULL
    ORDER BY posicion_presupuestaria, rubro
)
SELECT
    COALESCE(p.red, e.red)                                         AS red,
    COALESCE(p.categoria,
             CASE WHEN COALESCE(p.red, e.red) LIKE 'SCENTRAL%' THEN 'SEDE CENTRAL'
                  WHEN COALESCE(p.red, e.red) = 'SIN RED'      THEN NULL
                  ELSE 'REDES' END)                                AS categoria,
    COALESCE(p.posicion_presupuestaria, e.posicion_presupuestaria) AS posicion_presupuestaria,
    COALESCE(pp.descripcion, p.concepto_pia,
             p.posicion_presupuestaria, e.posicion_presupuestaria) AS desc_posicion,
    COALESCE(p.rubro, rp.rubro)                                    AS rubro,
    COALESCE(p.tipo_gasto, rp.cat2)                                AS tipo_gasto,
    COALESCE(p.pia, 0)                                             AS pia,
    COALESCE(e.pres_modificado, 0)                                 AS pres_modificado,
    COALESCE(e.pres_asignable, 0)                                  AS pres_asignable,
    COALESCE(e.certificacion, 0)                                   AS certificacion,
    COALESCE(e.pres_comprometido, 0)                               AS pres_comprometido,
    COALESCE(e.pres_ejecutado, 0)                                  AS pres_ejecutado,
    COALESCE(e.saldo_sin_asignar, 0)                               AS saldo_sin_asignar,
    COALESCE(e.pres_modificado, 0) - COALESCE(p.pia, 0)            AS modificacion_vs_pia,
    ROUND(e.pres_ejecutado / NULLIF(p.pia, 0), 4)                  AS avance_pia,
    ROUND(e.pres_ejecutado / NULLIF(e.pres_modificado, 0), 4)      AS avance_pim,
    e.n_centros_gestores,
    e.n_fondos,
    e.fondos,
    CASE WHEN p.red IS NULL THEN 'SOLO EJECUCION'
         WHEN e.red IS NULL THEN 'SOLO PIA'
         ELSE 'PIA Y EJECUCION' END                                AS origen,
    now()                                                          AS fecha_actualizacion
FROM pia p
FULL OUTER JOIN eje e
       ON e.red = p.red
      AND e.posicion_presupuestaria = p.posicion_presupuestaria
LEFT JOIN dwsge.pre_posiciones pp
       ON pp.posicion_presupuestaria = COALESCE(p.posicion_presupuestaria, e.posicion_presupuestaria)
LEFT JOIN rubro_pos rp
       ON rp.posicion_presupuestaria = COALESCE(p.posicion_presupuestaria, e.posicion_presupuestaria);

-- Índice único: permite REFRESH MATERIALIZED VIEW CONCURRENTLY
CREATE UNIQUE INDEX ux_mv_pre_pia_ejecucion
    ON dwsge.mv_pre_pia_ejecucion (red, posicion_presupuestaria);
CREATE INDEX ix_mv_pre_pia_ejecucion_rubro
    ON dwsge.mv_pre_pia_ejecucion (rubro);
