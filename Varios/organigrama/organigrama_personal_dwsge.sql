-- =====================================================================================
-- ORGANIGRAMA + PERSONAL -> esquema dwsge
-- Vista materializada: dwsge.vw_organigrama_jerarquia_personal
--   Todas las columnas de dwsge.vw_organigrama_jerarquia más una fila por persona (nro_personal) de cada unidad,
--   con periodo, sociedad, grupo de personal, área de personal, posición, sexo, grupo profesional, nombre,
--   función y clase de contrato. cantidad_personal = 1 (sumar para contar).
--   Solo usa el último periodo cargado en data_personal (max(periodo) al momento del REFRESH);
--   cargar_data_personal.py la refresca al terminar cada carga.
--
-- Personal: dwsge.data_personal, solo Sociedad = 'Fondo AFIPSS' o 'Fondo Salud' y División de personal
--           'SEDE CENTRAL LIMA-…' (se excluye el personal de redes aunque su centro de costo esté homologado).
-- Cruce   : data_personal -> centro_de_costo_afessalud_homologado por ce_coste (único en la homologación,
--           índice ux_cc_afessalud_ce_coste); así cada trabajador cae en una sola unidad.
--           Si una persona viene dos veces (DNI y carné de extranjería) se queda la fila de fecha más reciente.
-- El personal de Sede Central sin centro de costo (o con uno no homologado) también aparece, con las columnas
-- del organigrama (id_unidad, codigo, nombre, ...) en NULL; ver consulta de control.
--
-- Requiere que existan organigrama_unidad (organigrama_dwsge.sql), centro_de_costo_afessalud_homologado
-- (cargar_centro_costo.py) y data_personal (cargar_data_personal.py). Es idempotente.
-- Ejecucion:  psql -h <host> -p <port> -U <user> -d DW_ESTADISTICA -v ON_ERROR_STOP=1 -f organigrama_personal_dwsge.sql
-- Refresco :  REFRESH MATERIALIZED VIEW dwsge.vw_organigrama_jerarquia_personal;
-- =====================================================================================
SET client_encoding = 'UTF8';

BEGIN;

DROP MATERIALIZED VIEW IF EXISTS dwsge.vw_organigrama_jerarquia_personal;

CREATE MATERIALIZED VIEW dwsge.vw_organigrama_jerarquia_personal AS
WITH personal AS (
    SELECT DISTINCT ON (c.id_unidad, p.periodo, p.nro_personal)
           c.id_unidad,
           p.periodo,
           p.sociedad,
           p.grupo_personal,
           p.area_personal,
           p.posicion,
           p.sexo,
           p.cod_grupo_profesional,
           p.nro_personal,
           p.nombre_personal,
           p.funcion,
           p.clase_contrato
    FROM   dwsge.data_personal p
    LEFT   JOIN dwsge.centro_de_costo_afessalud_homologado c
           ON  c.ce_coste = p.ce_coste
    WHERE  p.periodo = (SELECT max(periodo) FROM dwsge.data_personal)
      AND  p.sociedad IN ('Fondo AFIPSS', 'Fondo Salud')
      AND  p.division_personal LIKE 'SEDE CENTRAL LIMA%'
    ORDER  BY c.id_unidad, p.periodo, p.nro_personal, p.fecha DESC NULLS LAST, p.id_data_personal DESC
)
SELECT v.*,
       pe.periodo,
       pe.sociedad,
       pe.grupo_personal,
       pe.area_personal,
       pe.posicion,
       pe.sexo,
       pe.cod_grupo_profesional,
       1::bigint AS cantidad_personal,
       pe.nro_personal,
       pe.nombre_personal,
       pe.funcion,
       pe.clase_contrato
FROM   personal pe
LEFT   JOIN dwsge.vw_organigrama_jerarquia v ON v.id_unidad = pe.id_unidad;

CREATE UNIQUE INDEX ux_vw_organigrama_jerarquia_personal
    ON dwsge.vw_organigrama_jerarquia_personal (id_unidad, periodo, nro_personal);

COMMENT ON MATERIALIZED VIEW dwsge.vw_organigrama_jerarquia_personal IS
    'Una fila por persona (nro_personal), unidad del organigrama y periodo, solo del último periodo cargado. Personal sin centro de costo homologado: columnas del organigrama en NULL. cantidad_personal = 1 (sumar para contar). Fuente: dwsge.data_personal (Fondo AFIPSS/Fondo Salud, Sede Central Lima).';

COMMIT;

-- Control: personal de Sede Central (AFIPSS / Fondo Salud) del último periodo sin unidad del organigrama (aparece en la vista con id_unidad NULL)
-- SELECT p.periodo, p.sociedad, count(*) AS personas, count(DISTINCT p.ce_coste) AS centros_de_costo
-- FROM dwsge.data_personal p
-- WHERE p.periodo = (SELECT max(periodo) FROM dwsge.data_personal)
--   AND p.sociedad IN ('Fondo AFIPSS', 'Fondo Salud')
--   AND p.division_personal LIKE 'SEDE CENTRAL LIMA%'
--   AND NOT EXISTS (SELECT 1 FROM dwsge.centro_de_costo_afessalud_homologado c
--                   WHERE c.ce_coste = p.ce_coste)
-- GROUP BY 1, 2;
