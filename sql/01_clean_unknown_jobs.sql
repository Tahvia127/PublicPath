-- ============================================================
-- SQL BLOCKS 1-6: Clean 4,694 "unknown" org_type jobs
-- Run each block in Supabase SQL Editor
-- ============================================================

-- BLOCK 1: Reclassify federal agencies still marked "unknown"
UPDATE jobs
SET organization_type = 'federal'
WHERE organization_type = 'unknown'
  AND is_active = true
  AND (
    organization ILIKE '%department of%'
    OR organization ILIKE '%u.s. %'
    OR organization ILIKE '%us army%'
    OR organization ILIKE '%us navy%'
    OR organization ILIKE '%us air force%'
    OR organization ILIKE '%federal bureau%'
    OR organization ILIKE '%national%institute%'
    OR organization ILIKE '%centers for disease%'
    OR organization ILIKE '%social security%'
    OR organization ILIKE '%veterans affairs%'
    OR organization ILIKE '%dept of%'
    OR organization ILIKE '%office of%management%'
    OR title ILIKE '%federal%'
    OR description ILIKE '%federal agency%'
    OR description ILIKE '%federal government%'
  );

-- BLOCK 2: Reclassify state government jobs still marked "unknown"
UPDATE jobs
SET organization_type = 'state'
WHERE organization_type = 'unknown'
  AND is_active = true
  AND (
    organization ILIKE 'state of %'
    OR organization ILIKE '% state agency%'
    OR organization ILIKE '% state department%'
    OR organization ILIKE 'commonwealth of %'
    OR organization ILIKE '% state police%'
    OR organization ILIKE '% state university%'
    OR organization ILIKE '% state college%'
    OR description ILIKE '%state government%'
    OR description ILIKE '%state agency%'
  );

-- BLOCK 3: Reclassify local/municipal government jobs still marked "unknown"
UPDATE jobs
SET organization_type = 'local'
WHERE organization_type = 'unknown'
  AND is_active = true
  AND (
    organization ILIKE 'city of %'
    OR organization ILIKE 'county of %'
    OR organization ILIKE 'town of %'
    OR organization ILIKE 'village of %'
    OR organization ILIKE '% county %'
    OR organization ILIKE '% city %'
    OR organization ILIKE '% municipal%'
    OR organization ILIKE '% metro %'
    OR organization ILIKE '% district%'
    OR organization ILIKE '% transit authority%'
    OR organization ILIKE '% school district%'
    OR description ILIKE '%local government%'
    OR description ILIKE '%municipal government%'
  );

-- BLOCK 4: Reclassify nonprofits still marked "unknown"
UPDATE jobs
SET organization_type = 'nonprofit'
WHERE organization_type = 'unknown'
  AND is_active = true
  AND (
    organization ILIKE '%foundation%'
    OR organization ILIKE '%institute%'
    OR organization ILIKE '%association%'
    OR organization ILIKE '%alliance%'
    OR organization ILIKE '%coalition%'
    OR organization ILIKE '%council%'
    OR organization ILIKE '%nonprofit%'
    OR organization ILIKE '%non-profit%'
    OR organization ILIKE '%ngo%'
    OR organization ILIKE '%fund%'
    OR description ILIKE '%501(c)%'
    OR description ILIKE '% nonprofit %'
    OR description ILIKE '%non-profit organization%'
  );

-- BLOCK 5: Deactivate clearly private-sector "unknown" jobs (tech companies, etc.)
UPDATE jobs
SET is_active = false
WHERE organization_type = 'unknown'
  AND is_active = true
  AND source IN ('jooble', 'adzuna', 'findwork', 'jobicy')
  AND (
    organization ILIKE '%amazon%'
    OR organization ILIKE '%google%'
    OR organization ILIKE '%microsoft%'
    OR organization ILIKE '%apple%'
    OR organization ILIKE '%meta%'
    OR organization ILIKE '%netflix%'
    OR organization ILIKE '%uber%'
    OR organization ILIKE '%lyft%'
    OR organization ILIKE '%airbnb%'
    OR organization ILIKE '%stripe%'
    OR organization ILIKE '%salesforce%'
    OR organization ILIKE '%oracle%'
    OR organization ILIKE '%ibm%'
    OR organization ILIKE '%accenture%'
    OR organization ILIKE '%deloitte%'
    OR organization ILIKE '%mckinsey%'
    OR organization ILIKE '%bain%'
    OR organization ILIKE '%bcg%'
    OR organization ILIKE '%jpmorgan%'
    OR organization ILIKE '%goldman sachs%'
    OR organization ILIKE '%morgan stanley%'
  );

-- BLOCK 6: Count remaining "unknown" jobs after cleanup (verify results)
SELECT
  COUNT(*) FILTER (WHERE organization_type = 'unknown' AND is_active = true)  AS remaining_unknown,
  COUNT(*) FILTER (WHERE organization_type = 'federal' AND is_active = true)  AS federal,
  COUNT(*) FILTER (WHERE organization_type = 'state' AND is_active = true)    AS state,
  COUNT(*) FILTER (WHERE organization_type = 'local' AND is_active = true)    AS local,
  COUNT(*) FILTER (WHERE organization_type = 'nonprofit' AND is_active = true) AS nonprofit,
  COUNT(*) FILTER (WHERE is_active = true)                                      AS total_active
FROM jobs;
