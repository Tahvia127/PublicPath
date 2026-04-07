-- ============================================================
-- dedup_cross_source_jobs() v2 — title + state similarity dedup
--
-- WHY v1 RETURNED 0:
-- The original function grouped by fingerprint = MD5(title|org|state).
-- Cross-source duplicates have different org names (e.g. "Dept of Energy"
-- vs "Department of Energy" vs "DOE"), so fingerprints never match.
--
-- THIS VERSION groups by MD5(normalized_title|state), ignoring org name.
-- This catches the same job posted by USAJobs + Jooble + Adzuna.
--
-- Run in Supabase SQL Editor — replaces the old function.
-- ============================================================

CREATE OR REPLACE FUNCTION dedup_cross_source_jobs()
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  deactivated_count INTEGER := 0;
BEGIN
  WITH source_priority(src, priority) AS (
    VALUES
      ('usajobs',     1),
      ('jooble',      2),
      ('adzuna',      3),
      ('google_jobs', 4),
      ('findwork',    5),
      ('jobicy',      6),
      ('careerjet',   7)
  ),
  -- Compute a title-based dedup key: lowercase, strip punctuation, collapse spaces
  normalized AS (
    SELECT
      j.id,
      j.source,
      j.is_active,
      MD5(
        REGEXP_REPLACE(
          LOWER(COALESCE(j.title, '')),
          '[^a-z0-9 ]', '', 'g'         -- strip punctuation
        )
        || '|'
        || LOWER(COALESCE(j.location_state, ''))
      ) AS title_state_key,
      COALESCE(sp.priority, 99) AS priority,
      j.created_at
    FROM jobs j
    LEFT JOIN source_priority sp ON sp.src = j.source
    WHERE j.is_active = true
  ),
  -- Rank within each title+state group, best source first
  ranked AS (
    SELECT
      id,
      title_state_key,
      priority,
      ROW_NUMBER() OVER (
        PARTITION BY title_state_key
        ORDER BY priority ASC, created_at ASC
      ) AS rn
    FROM normalized
    WHERE title_state_key IS NOT NULL
  )
  -- Deactivate all but rank 1 (the highest-priority source's version)
  UPDATE jobs
  SET is_active = false
  WHERE id IN (
    SELECT id FROM ranked WHERE rn > 1
  );

  GET DIAGNOSTICS deactivated_count = ROW_COUNT;
  RETURN deactivated_count;
END;
$$;

GRANT EXECUTE ON FUNCTION dedup_cross_source_jobs() TO service_role;
GRANT EXECUTE ON FUNCTION dedup_cross_source_jobs() TO authenticated;

-- Run the updated function
SELECT dedup_cross_source_jobs() AS deactivated_cross_source_dupes;

-- Verify: show remaining active job counts by source
SELECT source, COUNT(*) AS active_jobs
FROM jobs
WHERE is_active = true
GROUP BY source
ORDER BY active_jobs DESC;
