-- ============================================================
-- BLOCK 8: Deactivate "unknown" jobs with no credible public-sector signal
--
-- Strategy: for jobs still marked "unknown" after passes 1 & 2,
-- deactivate any that have NO public-sector keywords in title OR description.
-- These are private-sector jobs that slipped through Jooble/Adzuna filters.
--
-- Safe: only touches organization_type = 'unknown' AND is_active = true
-- Run in Supabase SQL Editor
-- ============================================================

-- BLOCK 8A: Deactivate unknowns from aggregator sources (Jooble, Adzuna)
-- that have no public-sector signal in title or description
UPDATE jobs
SET is_active = false
WHERE organization_type = 'unknown'
  AND is_active = true
  AND source IN ('jooble', 'adzuna', 'findwork', 'jobicy', 'google_jobs')
  AND NOT (
    -- Title signals
    title ILIKE '%government%'
    OR title ILIKE '%public sector%'
    OR title ILIKE '%public policy%'
    OR title ILIKE '%public health%'
    OR title ILIKE '%public administration%'
    OR title ILIKE '%municipal%'
    OR title ILIKE '%federal%'
    OR title ILIKE '%nonprofit%'
    OR title ILIKE '%non-profit%'
    OR title ILIKE '%city of%'
    OR title ILIKE '%county%'
    OR title ILIKE '%state agency%'
    OR title ILIKE '%legislature%'
    OR title ILIKE '%legislative%'
    OR title ILIKE '%policy analyst%'
    OR title ILIKE '%policy advisor%'
    OR title ILIKE '%policy director%'
    OR title ILIKE '%policy manager%'
    OR title ILIKE '%policy officer%'
    OR title ILIKE '%civic%'
    OR title ILIKE '%transit%'
    OR title ILIKE '%housing authority%'
    OR title ILIKE '%public works%'
    -- Description signals
    OR description ILIKE '%government%'
    OR description ILIKE '%public sector%'
    OR description ILIKE '%federal agency%'
    OR description ILIKE '%federal employee%'
    OR description ILIKE '%state government%'
    OR description ILIKE '%local government%'
    OR description ILIKE '%city government%'
    OR description ILIKE '%county government%'
    OR description ILIKE '%nonprofit%'
    OR description ILIKE '%non-profit%'
    OR description ILIKE '%501(c)%'
    OR description ILIKE '%public administration%'
    OR description ILIKE '%public policy%'
    OR description ILIKE '%public health%'
    OR description ILIKE '%public interest%'
    OR description ILIKE '%usajobs%'
    OR description ILIKE '%general schedule%'
    OR description ILIKE '% gs-%'
    OR description ILIKE '%civil service%'
    OR description ILIKE '%municipal%'
    OR description ILIKE '%legislature%'
    OR description ILIKE '%legislative%'
    -- Org name signals (last resort)
    OR organization ILIKE '%department%'
    OR organization ILIKE '%bureau%'
    OR organization ILIKE '%commission%'
    OR organization ILIKE '%authority%'
    OR organization ILIKE '%agency%'
    OR organization ILIKE '%foundation%'
    OR organization ILIKE '%institute%'
    OR organization ILIKE '%county%'
    OR organization ILIKE '%city of%'
  );

-- BLOCK 8B: Final count — verify we hit <500 unknown target
SELECT
  COUNT(*) FILTER (WHERE organization_type = 'unknown' AND is_active = true)   AS remaining_unknown,
  COUNT(*) FILTER (WHERE organization_type = 'federal' AND is_active = true)   AS federal,
  COUNT(*) FILTER (WHERE organization_type = 'state' AND is_active = true)     AS state,
  COUNT(*) FILTER (WHERE organization_type = 'local' AND is_active = true)     AS local,
  COUNT(*) FILTER (WHERE organization_type = 'nonprofit' AND is_active = true) AS nonprofit,
  COUNT(*) FILTER (WHERE is_active = true)                                       AS total_active,
  COUNT(*) FILTER (WHERE is_active = false)                                      AS total_inactive
FROM jobs;

-- BLOCK 8C: Breakdown of remaining unknowns by source (should be mostly usajobs)
SELECT source, COUNT(*) AS remaining_unknown
FROM jobs
WHERE organization_type = 'unknown'
  AND is_active = true
GROUP BY source
ORDER BY remaining_unknown DESC;
