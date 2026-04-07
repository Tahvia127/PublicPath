-- ============================================================
-- PASS 2: Reclassify remaining "unknown" org_type jobs
-- More aggressive — uses description + title text matching
-- Run in Supabase SQL Editor after 01_clean_unknown_jobs.sql
-- ============================================================

-- BLOCK A: Federal — catch agencies missed in pass 1 via description keywords
UPDATE jobs
SET organization_type = 'federal'
WHERE organization_type = 'unknown'
  AND is_active = true
  AND (
    description ILIKE '%federal government%'
    OR description ILIKE '%u.s. government%'
    OR description ILIKE '%united states government%'
    OR description ILIKE '%department of defense%'
    OR description ILIKE '%department of state%'
    OR description ILIKE '%department of justice%'
    OR description ILIKE '%department of energy%'
    OR description ILIKE '%department of health%'
    OR description ILIKE '%department of labor%'
    OR description ILIKE '%department of housing%'
    OR description ILIKE '%department of treasury%'
    OR description ILIKE '%department of commerce%'
    OR description ILIKE '%department of interior%'
    OR description ILIKE '%department of education%'
    OR description ILIKE '%department of agriculture%'
    OR description ILIKE '%department of homeland%'
    OR description ILIKE '%federal bureau%'
    OR description ILIKE '%federal agency%'
    OR description ILIKE '%federal employee%'
    OR description ILIKE '%general schedule%'
    OR description ILIKE '% GS-% position%'
    OR description ILIKE '%usajobs%'
    OR description ILIKE '%opm.gov%'
    OR description ILIKE '%competitive service%'
    OR description ILIKE '%excepted service%'
    OR title ILIKE '%federal%analyst%'
    OR title ILIKE '%federal%officer%'
    OR title ILIKE '%federal%specialist%'
  );

-- BLOCK B: State — catch via description
UPDATE jobs
SET organization_type = 'state'
WHERE organization_type = 'unknown'
  AND is_active = true
  AND (
    description ILIKE '%state of illinois%'
    OR description ILIKE '%state of new york%'
    OR description ILIKE '%state of california%'
    OR description ILIKE '%state of virginia%'
    OR description ILIKE '%state of maryland%'
    OR description ILIKE '%state of washington%'
    OR description ILIKE '%state of texas%'
    OR description ILIKE '%state of florida%'
    OR description ILIKE '%state of georgia%'
    OR description ILIKE '%state of ohio%'
    OR description ILIKE '%state of michigan%'
    OR description ILIKE '%state of pennsylvania%'
    OR description ILIKE '%state of massachusetts%'
    OR description ILIKE '%state of colorado%'
    OR description ILIKE '%state of arizona%'
    OR description ILIKE '%state of minnesota%'
    OR description ILIKE '%state of north carolina%'
    OR description ILIKE '%state employee%'
    OR description ILIKE '%state civil service%'
    OR description ILIKE '%state legislature%'
    OR description ILIKE '%state capitol%'
    OR description ILIKE '%governor%office%'
    OR description ILIKE '%department of transportation%'
    OR description ILIKE '%state board of%'
    OR description ILIKE '%state commission%'
    OR title ILIKE '%state policy%'
    OR organization ILIKE '% state%'
  );

-- BLOCK C: Local — catch via description and org patterns
UPDATE jobs
SET organization_type = 'local'
WHERE organization_type = 'unknown'
  AND is_active = true
  AND (
    description ILIKE '%city of chicago%'
    OR description ILIKE '%city of new york%'
    OR description ILIKE '%city of los angeles%'
    OR description ILIKE '%city of houston%'
    OR description ILIKE '%city of washington%'
    OR description ILIKE '%city of boston%'
    OR description ILIKE '%city of seattle%'
    OR description ILIKE '%city of denver%'
    OR description ILIKE '%city of atlanta%'
    OR description ILIKE '%city of philadelphia%'
    OR description ILIKE '%city of san francisco%'
    OR description ILIKE '%city of dallas%'
    OR description ILIKE '%city of austin%'
    OR description ILIKE '%city of phoenix%'
    OR description ILIKE '%city of miami%'
    OR description ILIKE '%county government%'
    OR description ILIKE '%county board%'
    OR description ILIKE '%county commission%'
    OR description ILIKE '%local government%'
    OR description ILIKE '%public works%'
    OR description ILIKE '%parks and recreation%'
    OR description ILIKE '%city council%'
    OR description ILIKE '%mayor%office%'
    OR description ILIKE '%transit authority%'
    OR description ILIKE '%port authority%'
    OR description ILIKE '%housing authority%'
    OR organization ILIKE '% county'
    OR organization ILIKE '% county %'
    OR organization ILIKE '%metropolitan%'
    OR organization ILIKE '%transit%authority%'
    OR organization ILIKE '%port authority%'
    OR organization ILIKE '%housing authority%'
    OR organization ILIKE '%water authority%'
    OR organization ILIKE '%public works%'
  );

-- BLOCK D: Nonprofit — catch via description
UPDATE jobs
SET organization_type = 'nonprofit'
WHERE organization_type = 'unknown'
  AND is_active = true
  AND (
    description ILIKE '%nonprofit organization%'
    OR description ILIKE '%non-profit organization%'
    OR description ILIKE '%501(c)(3)%'
    OR description ILIKE '%501c3%'
    OR description ILIKE '%public interest%'
    OR description ILIKE '%civil society%'
    OR description ILIKE '%advocacy organization%'
    OR description ILIKE '%mission-driven%'
    OR description ILIKE '%mission driven%'
    OR description ILIKE '%social impact%'
    OR description ILIKE '%community organization%'
    OR description ILIKE '%charitable organization%'
    OR description ILIKE '%philanthropic%'
    OR organization ILIKE '%center for%'
    OR organization ILIKE '%institute for%'
    OR organization ILIKE '%action network%'
    OR organization ILIKE '%public interest%'
  );

-- BLOCK E: Deactivate jobs with no location AND no clear public sector signal
-- These are low-quality entries unlikely to be useful to users
UPDATE jobs
SET is_active = false
WHERE organization_type = 'unknown'
  AND is_active = true
  AND (location_state IS NULL OR location_state = '')
  AND (location_city IS NULL OR location_city = '')
  AND is_remote = false
  AND source IN ('jooble', 'adzuna');

-- BLOCK F: Final count — verify progress
SELECT
  COUNT(*) FILTER (WHERE organization_type = 'unknown' AND is_active = true)   AS remaining_unknown,
  COUNT(*) FILTER (WHERE organization_type = 'federal' AND is_active = true)   AS federal,
  COUNT(*) FILTER (WHERE organization_type = 'state' AND is_active = true)     AS state,
  COUNT(*) FILTER (WHERE organization_type = 'local' AND is_active = true)     AS local,
  COUNT(*) FILTER (WHERE organization_type = 'nonprofit' AND is_active = true) AS nonprofit,
  COUNT(*) FILTER (WHERE is_active = true)                                       AS total_active
FROM jobs;
