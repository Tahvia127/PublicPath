-- ============================================================
-- dedup_cross_source_jobs() RPC function
-- Deactivates duplicate jobs across sources, keeping the best one
-- Priority: usajobs > jooble > adzuna > google_jobs > findwork > jobicy
-- Run in Supabase SQL Editor
-- ============================================================

CREATE OR REPLACE FUNCTION dedup_cross_source_jobs()
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  deactivated_count INTEGER := 0;
BEGIN
  -- Source priority order (lower number = higher priority / keep this one)
  WITH source_priority AS (
    SELECT unnest(ARRAY['usajobs','jooble','adzuna','google_jobs','findwork','jobicy','careerjet']) AS source,
           generate_subscripts(ARRAY['usajobs','jooble','adzuna','google_jobs','findwork','jobicy','careerjet'], 1) AS priority
  ),
  -- Find all active jobs that share a fingerprint with another active job
  dupes AS (
    SELECT
      j.id,
      j.fingerprint,
      j.source,
      j.created_at,
      COALESCE(sp.priority, 99) AS priority,
      ROW_NUMBER() OVER (
        PARTITION BY j.fingerprint
        ORDER BY COALESCE(sp.priority, 99), j.created_at ASC
      ) AS rn
    FROM jobs j
    LEFT JOIN source_priority sp ON sp.source = j.source
    WHERE j.is_active = true
      AND j.fingerprint IS NOT NULL
      AND j.fingerprint != ''
  )
  -- Deactivate all but the highest-priority duplicate
  UPDATE jobs
  SET is_active = false
  WHERE id IN (
    SELECT id FROM dupes WHERE rn > 1
  );

  GET DIAGNOSTICS deactivated_count = ROW_COUNT;

  RETURN deactivated_count;
END;
$$;

-- Grant execute permission to authenticated and service roles
GRANT EXECUTE ON FUNCTION dedup_cross_source_jobs() TO service_role;
GRANT EXECUTE ON FUNCTION dedup_cross_source_jobs() TO authenticated;

-- Test: run the function and see how many dupes were deactivated
SELECT dedup_cross_source_jobs() AS deactivated_jobs;
