-- ============================================================
-- Add fingerprint column to jobs table (if not already present)
-- Run in Supabase SQL Editor
-- ============================================================

-- Step 1: Add the fingerprint column (safe to run even if column exists via IF NOT EXISTS)
ALTER TABLE jobs
  ADD COLUMN IF NOT EXISTS fingerprint TEXT;

-- Step 2: Backfill fingerprints for all existing jobs that don't have one
-- Uses MD5 of lowercase(title|organization|location_state)
UPDATE jobs
SET fingerprint = MD5(
  LOWER(COALESCE(title, ''))
  || '|'
  || LOWER(COALESCE(organization, ''))
  || '|'
  || LOWER(COALESCE(location_state, ''))
)
WHERE fingerprint IS NULL;

-- Step 3: Create an index on fingerprint for fast dedup lookups
CREATE INDEX IF NOT EXISTS idx_jobs_fingerprint ON jobs(fingerprint);

-- Step 4: Create an index on is_active for fast filtering
CREATE INDEX IF NOT EXISTS idx_jobs_is_active ON jobs(is_active);

-- Step 5: Verify - show count of jobs with/without fingerprints
SELECT
  COUNT(*) FILTER (WHERE fingerprint IS NOT NULL) AS with_fingerprint,
  COUNT(*) FILTER (WHERE fingerprint IS NULL)     AS without_fingerprint,
  COUNT(*)                                         AS total
FROM jobs;
