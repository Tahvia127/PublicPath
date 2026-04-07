-- ============================================================
-- DIAGNOSTIC: Understand the remaining 5,731 unknown jobs
-- Run each block separately — results will guide the fix strategy
-- ============================================================

-- DIAGNOSTIC 1: Where are the unknowns coming from?
-- If Jooble/Adzuna dominate, we can apply stricter public-sector filters
SELECT
  source,
  COUNT(*) AS unknown_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM jobs
WHERE organization_type = 'unknown'
  AND is_active = true
GROUP BY source
ORDER BY unknown_count DESC;

-- ============================================================

-- DIAGNOSTIC 2: Sample 20 unknown jobs from Jooble — see what titles/orgs look like
SELECT title, organization, location_state, LEFT(description, 120) AS desc_snippet
FROM jobs
WHERE organization_type = 'unknown'
  AND is_active = true
  AND source = 'jooble'
LIMIT 20;

-- ============================================================

-- DIAGNOSTIC 3: Sample 20 unknown jobs from Adzuna
SELECT title, organization, location_state, LEFT(description, 120) AS desc_snippet
FROM jobs
WHERE organization_type = 'unknown'
  AND is_active = true
  AND source = 'adzuna'
LIMIT 20;

-- ============================================================

-- DIAGNOSTIC 4: What are the most common organizations among unknowns?
-- If you see private companies, we can blocklist them
SELECT
  organization,
  COUNT(*) AS cnt
FROM jobs
WHERE organization_type = 'unknown'
  AND is_active = true
  AND organization IS NOT NULL
  AND organization != ''
GROUP BY organization
ORDER BY cnt DESC
LIMIT 40;
