-- ============================================================
-- FINAL PASS: Get remaining unknowns under 500
-- Run blocks in order in Supabase SQL Editor
-- ============================================================

-- DIAGNOSTIC: Top orgs still in the unknown pool by source
-- Run this first to see what's left — do NOT run the UPDATE blocks until you've seen this
SELECT
  source,
  organization,
  COUNT(*) AS cnt
FROM jobs
WHERE organization_type = 'unknown'
  AND is_active = true
  AND source IN ('jooble', 'adzuna')
GROUP BY source, organization
ORDER BY source, cnt DESC
LIMIT 60;

-- ============================================================
-- BLOCK 10A: Deactivate Jooble unknowns — staffing/contractor patterns
-- that block 9D missed because the org name didn't match the pattern
UPDATE jobs
SET is_active = false
WHERE organization_type = 'unknown'
  AND is_active = true
  AND source = 'jooble'
  AND NOT (
    -- Keep if strong public-sector signal in title
    title ILIKE '%government%'
    OR title ILIKE '%public sector%'
    OR title ILIKE '%public policy%'
    OR title ILIKE '%public health%'
    OR title ILIKE '%public administration%'
    OR title ILIKE '%federal%'
    OR title ILIKE '%municipal%'
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
    OR title ILIKE '%public works%'
    OR title ILIKE '%social worker%'
    OR title ILIKE '%caseworker%'
    -- Keep if strong public-sector signal in description
    OR description ILIKE '%government%'
    OR description ILIKE '%public sector%'
    OR description ILIKE '%federal agency%'
    OR description ILIKE '%state government%'
    OR description ILIKE '%local government%'
    OR description ILIKE '%city government%'
    OR description ILIKE '%county government%'
    OR description ILIKE '%nonprofit%'
    OR description ILIKE '%non-profit%'
    OR description ILIKE '%501(c)%'
    OR description ILIKE '%public administration%'
    OR description ILIKE '%public policy%'
    OR description ILIKE '%public health department%'
    OR description ILIKE '%public interest%'
    OR description ILIKE '%usajobs%'
    OR description ILIKE '%general schedule%'
    OR description ILIKE '% gs-%'
    OR description ILIKE '%civil service%'
    OR description ILIKE '%municipal%'
    OR description ILIKE '%legislature%'
    -- Keep if org name signals public sector
    OR organization ILIKE '%department%'
    OR organization ILIKE '%bureau%'
    OR organization ILIKE '%commission%'
    OR organization ILIKE '%authority%'
    OR organization ILIKE '%county%'
    OR organization ILIKE '%city of%'
    OR organization ILIKE '%state of%'
    OR organization ILIKE '%foundation%'
    OR organization ILIKE '%institute%'
    OR organization ILIKE '%university%'
    OR organization ILIKE '%college%'
    OR organization ILIKE '%school%'
    OR organization ILIKE '%health%'
    OR organization ILIKE '%hospital%'
    OR organization ILIKE '%council%'
    OR organization ILIKE '%coalition%'
    OR organization ILIKE '%association%'
    OR organization ILIKE '%nonprofit%'
    OR organization ILIKE '%non-profit%'
    OR organization ILIKE '%government%'
    OR organization ILIKE '%agency%'
    OR organization ILIKE 'NYC%'
    OR organization ILIKE 'CUNY%'
    OR organization ILIKE '%public%'
  );

-- BLOCK 10B: Deactivate Adzuna unknowns using the same broad keep-list
UPDATE jobs
SET is_active = false
WHERE organization_type = 'unknown'
  AND is_active = true
  AND source = 'adzuna'
  AND NOT (
    title ILIKE '%government%'
    OR title ILIKE '%public sector%'
    OR title ILIKE '%public policy%'
    OR title ILIKE '%public health%'
    OR title ILIKE '%public administration%'
    OR title ILIKE '%federal%'
    OR title ILIKE '%municipal%'
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
    OR title ILIKE '%public works%'
    OR title ILIKE '%social worker%'
    OR title ILIKE '%caseworker%'
    OR description ILIKE '%government%'
    OR description ILIKE '%public sector%'
    OR description ILIKE '%federal agency%'
    OR description ILIKE '%state government%'
    OR description ILIKE '%local government%'
    OR description ILIKE '%city government%'
    OR description ILIKE '%county government%'
    OR description ILIKE '%nonprofit%'
    OR description ILIKE '%non-profit%'
    OR description ILIKE '%501(c)%'
    OR description ILIKE '%public administration%'
    OR description ILIKE '%public policy%'
    OR description ILIKE '%public health department%'
    OR description ILIKE '%public interest%'
    OR description ILIKE '%usajobs%'
    OR description ILIKE '%general schedule%'
    OR description ILIKE '% gs-%'
    OR description ILIKE '%civil service%'
    OR description ILIKE '%municipal%'
    OR description ILIKE '%legislature%'
    OR organization ILIKE '%department%'
    OR organization ILIKE '%bureau%'
    OR organization ILIKE '%commission%'
    OR organization ILIKE '%authority%'
    OR organization ILIKE '%county%'
    OR organization ILIKE '%city of%'
    OR organization ILIKE '%state of%'
    OR organization ILIKE '%foundation%'
    OR organization ILIKE '%institute%'
    OR organization ILIKE '%university%'
    OR organization ILIKE '%college%'
    OR organization ILIKE '%school%'
    OR organization ILIKE '%council%'
    OR organization ILIKE '%coalition%'
    OR organization ILIKE '%association%'
    OR organization ILIKE '%nonprofit%'
    OR organization ILIKE '%non-profit%'
    OR organization ILIKE '%government%'
    OR organization ILIKE '%agency%'
    OR organization ILIKE 'NYC%'
    OR organization ILIKE 'CUNY%'
    OR organization ILIKE '%public%'
  );

-- BLOCK 10C: Catch remaining themuse / arbeitnow / google_jobs unknowns
UPDATE jobs
SET is_active = false
WHERE organization_type = 'unknown'
  AND is_active = true
  AND source IN ('themuse', 'arbeitnow', 'google_jobs')
  AND NOT (
    description ILIKE '%government%'
    OR description ILIKE '%public sector%'
    OR description ILIKE '%nonprofit%'
    OR description ILIKE '%501(c)%'
    OR description ILIKE '%federal%'
    OR description ILIKE '%municipal%'
    OR organization ILIKE '%government%'
    OR organization ILIKE '%public%'
    OR organization ILIKE '%county%'
    OR organization ILIKE '%city of%'
    OR organization ILIKE '%nonprofit%'
  );

-- ============================================================
-- FINAL COUNT
-- ============================================================
SELECT
  COUNT(*) FILTER (WHERE organization_type = 'unknown' AND is_active = true)   AS remaining_unknown,
  COUNT(*) FILTER (WHERE organization_type = 'federal' AND is_active = true)   AS federal,
  COUNT(*) FILTER (WHERE organization_type = 'state' AND is_active = true)     AS state,
  COUNT(*) FILTER (WHERE organization_type = 'local' AND is_active = true)     AS local,
  COUNT(*) FILTER (WHERE organization_type = 'nonprofit' AND is_active = true) AS nonprofit,
  COUNT(*) FILTER (WHERE is_active = true)                                       AS total_active
FROM jobs;

-- Source breakdown of whatever remains
SELECT source, COUNT(*) AS remaining_unknown
FROM jobs
WHERE organization_type = 'unknown'
  AND is_active = true
GROUP BY source
ORDER BY remaining_unknown DESC;
