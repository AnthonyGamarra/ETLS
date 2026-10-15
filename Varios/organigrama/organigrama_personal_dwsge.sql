-- =====================================================================================
-- ORGANIGRAMA + PERSONAL -> esquema dwsge
-- Vista: dwsge.vw_organigrama_jerarquia_personal
--   Todas las columnas de dwsge.vw_organigrama_jerarquia más la cantidad de personal de cada unidad,
--   desagregada por periodo, sociedad, grupo de personal, área de personal, posición, sexo y grupo profesional.
--
-- Personal: dwsge.data_personal, solo Sociedad = 'Fondo AFIPSS' o 'Fondo Salud' y División de personal
--           'SEDE CENTRAL LIMA-…' (se excluye el personal de redes aunque su centro de costo esté homologado).
-- Cruce   : data_personal -> centro_de_costo_afessalud_homologado por ce_coste (único en la homologación,
--           índice ux_cc_afessalud_ce_coste); así cada trabajador cae en una sola unidad.
-- Solo trae unidades con personal: cada fila tiene periodo, sociedad y cantidad_personal > 0.
-- El personal cuyo centro de costo no está homologado (redes, hospitales) no aparece; ver consulta de control.
--
-- Requiere que existan organigrama_unidad (organigrama_dwsge.sql), centro_de_costo_afessalud_homologado
-- (cargar_centro_costo.py) y data_personal (cargar_data_personal.py). Es idempotente.
-- Ejecucion:  psql -h <host> -p <port> -U <user> -d DW_ESTADISTICA -v ON_ERROR_STOP=1 -f organigrama_personal_dwsge.sql
-- =====================================================================================
SET client_encoding = 'UTF8';

BEGIN;

DROP VIEW IF EXISTS dwsge.vw_organigrama_jerarquia_personal;

CREATE VIEW dwsge.vw_organigrama_jerarquia_personal AS
WITH personal AS (
    SELECT c.id_unidad,
           p.periodo,
           p.sociedad,
           p.grupo_personal,
           p.area_personal,
           p.posicion,
           p.sexo,
           p.cod_grupo_profesional,
           count(DISTINCT p.nro_personal) AS cantidad_personal
    FROM   dwsge.data_personal p
    JOIN   dwsge.centro_de_costo_afessalud_homologado c
           ON  c.ce_coste = p.ce_coste
    WHERE  p.sociedad IN ('Fondo AFIPSS', 'Fondo Salud')
      AND  p.division_personal LIKE 'SEDE CENTRAL LIMA%'
    GROUP  BY c.id_unidad, p.periodo, p.sociedad, p.grupo_personal, p.area_personal, p.posicion, p.sexo, p.cod_grupo_profesional
)
SELECT v.*,
       pe.periodo,
       pe.sociedad,
       pe.grupo_personal,
       pe.area_personal,
       pe.posicion,
       pe.sexo,
       pe.cod_grupo_profesional,
       pe.cantidad_personal
FROM   dwsge.vw_organigrama_jerarquia v
JOIN   personal pe ON pe.id_unidad = v.id_unidad;

COMMENT ON VIEW dwsge.vw_organigrama_jerarquia_personal IS
    'vw_organigrama_jerarquia + cantidad de personal Fondo AFIPSS y Fondo Salud de la Sede Central Lima por periodo, sociedad, grupo, área, posición, sexo y grupo profesional. Solo unidades con personal.';

COMMIT;

-- Control: personal de Sede Central (AFIPSS / Fondo Salud) cuyo centro de costo no está homologado (debe dar 0 filas)
-- SELECT p.periodo, p.sociedad, count(*) AS personas, count(DISTINCT p.ce_coste) AS centros_de_costo
-- FROM dwsge.data_personal p
-- WHERE p.sociedad IN ('Fondo AFIPSS', 'Fondo Salud')
--   AND p.division_personal LIKE 'SEDE CENTRAL LIMA%'
--   AND NOT EXISTS (SELECT 1 FROM dwsge.centro_de_costo_afessalud_homologado c
--                   WHERE c.ce_coste = p.ce_coste)
-- GROUP BY 1, 2;
