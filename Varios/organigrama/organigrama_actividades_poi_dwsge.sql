-- =====================================================================================
-- ORGANIGRAMA + ACTIVIDADES POI -> esquema dwsge
-- Vista: dwsge.vw_organigrama_actividades_poi
--   Una fila por actividad operativa del POI, con todas las columnas de dwsge.vw_organigrama_jerarquia
--   de la unidad a la que pertenece su centro de costo.
--
-- Cruce: actividades_poi -> centro_de_costo_afessalud_homologado por ce_coste (único en la homologación)
--        -> vw_organigrama_jerarquia por id_unidad. Solo trae actividades que cruzan.
-- Las metas (meta_i .. meta_iv) están en la unidad de medida de cada actividad: no sumarlas entre actividades.
-- Es independiente de vw_organigrama_jerarquia_personal; ambas se relacionan por id_unidad.
--
-- Requiere organigrama_unidad (organigrama_dwsge.sql), centro_de_costo_afessalud_homologado
-- (cargar_centro_costo.py) y actividades_poi (cargar_actividades_poi.py). Es idempotente.
-- Ejecucion:  psql -h <host> -p <port> -U <user> -d DW_ESTADISTICA -v ON_ERROR_STOP=1 -f organigrama_actividades_poi_dwsge.sql
-- =====================================================================================
SET client_encoding = 'UTF8';

BEGIN;

DROP VIEW IF EXISTS dwsge.vw_organigrama_actividades_poi;

CREATE VIEW dwsge.vw_organigrama_actividades_poi AS
SELECT v.*,
       a.id_actividad_poi,
       a.anio,
       a.organo_central,
       a.cod_objetivo_estrategico,
       a.cod_accion_estrategica,
       a.cod_centro_gestor,
       a.centro_gestor,
       a.ce_coste,
       a.centro_de_costo,
       a.cod_fondo_financiero,
       a.fondo_financiero,
       a.cod_actividad_operativa,
       a.actividad_operativa,
       a.unidad_medida,
       a.meta_i,
       a.meta_ii,
       a.meta_iii,
       a.meta_iv,
       a.remuneraciones,
       a.bienes,
       a.servicios,
       a.remuneraciones + a.bienes + a.servicios AS presupuesto_total
FROM   dwsge.actividades_poi a
JOIN   dwsge.centro_de_costo_afessalud_homologado c ON c.ce_coste = a.ce_coste
JOIN   dwsge.vw_organigrama_jerarquia v ON v.id_unidad = c.id_unidad;

COMMENT ON VIEW dwsge.vw_organigrama_actividades_poi IS
    'Actividades operativas del POI con la unidad del organigrama de su centro de costo. Una fila por actividad.';

COMMIT;
