-- ===================================================================
-- Serving views for universe exposure
-- ===================================================================
-- These views are intentionally simple in v1.
-- They project stable read-only serving objects from the core schema.
-- ===================================================================

CREATE OR REPLACE VIEW serving.universes AS
SELECT
    ud.universe_id,
    ud.universe_name,
    ud.description,
    ud.created_at,
    COUNT(DISTINCT umh.instrument_id) AS historical_instrument_count
FROM core.universe_definition AS ud
LEFT JOIN core.universe_membership_history AS umh
    ON ud.universe_id = umh.universe_id
GROUP BY
    ud.universe_id,
    ud.universe_name,
    ud.description,
    ud.created_at
ORDER BY
    ud.universe_name;
