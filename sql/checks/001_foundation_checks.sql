-- -------------------------------------------------------------------
-- Foundation integrity checks
--
-- These queries are intended to be executed by validation jobs.
-- In v1 they are simple probes. Later they can become blocking checks.
-- -------------------------------------------------------------------

-- Duplicate universe names should not exist.
SELECT
    universe_name,
    COUNT(*) AS duplicate_count
FROM core.universe_definition
GROUP BY universe_name
HAVING COUNT(*) > 1;

-- Duplicate instrument ids should not exist.
SELECT
    instrument_id,
    COUNT(*) AS duplicate_count
FROM core.instrument
GROUP BY instrument_id
HAVING COUNT(*) > 1;

-- Invalid temporal intervals in symbol_reference_history.
SELECT *
FROM core.symbol_reference_history
WHERE effective_to IS NOT NULL
  AND effective_to <= effective_from;

-- Invalid temporal intervals in listing_status_history.
SELECT *
FROM core.listing_status_history
WHERE effective_to IS NOT NULL
  AND effective_to <= effective_from;

-- Invalid temporal intervals in universe_membership_history.
SELECT *
FROM core.universe_membership_history
WHERE effective_to IS NOT NULL
  AND effective_to <= effective_from;
