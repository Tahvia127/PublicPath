-- ============================================================
-- SQL PASS 3: Blocklist private orgs + rescue legitimate public ones
-- Run in Supabase SQL Editor
-- ============================================================

-- BLOCK 9A: Rescue legitimate public sector orgs misclassified as "unknown"
-- These appeared in the top-40 org list and ARE public/civic sector
UPDATE jobs
SET organization_type = 'local'
WHERE organization_type = 'unknown'
  AND is_active = true
  AND organization IN (
    'NYC Health',
    'NYC Jobs',
    'Baltimore City',
    'CUNY'
  );

UPDATE jobs
SET organization_type = 'local'
WHERE organization_type = 'unknown'
  AND is_active = true
  AND (
    organization ILIKE 'NYC%'
    OR organization ILIKE 'Baltimore City%'
    OR organization ILIKE 'CUNY%'
    OR organization ILIKE 'City University%'
    OR organization ILIKE 'DC Health%'
    OR organization ILIKE 'DC Government%'
    OR organization ILIKE 'DC Office%'
    OR organization ILIKE 'Chicago Department%'
    OR organization ILIKE 'Chicago Public%'
    OR organization ILIKE 'LA County%'
    OR organization ILIKE 'Los Angeles County%'
    OR organization ILIKE 'Los Angeles City%'
    OR organization ILIKE 'New York City%'
    OR organization ILIKE 'NYC Department%'
    OR organization ILIKE 'King County%'
    OR organization ILIKE 'Cook County%'
    OR organization ILIKE 'Montgomery County%'
    OR organization ILIKE 'Prince George%'
    OR organization ILIKE 'Arlington County%'
    OR organization ILIKE 'Fairfax County%'
    OR organization ILIKE 'San Francisco%City%'
    OR organization ILIKE 'GovernmentJobs.com'
  );

-- BLOCK 9B: Reclassify government contractor job boards/aggregators
-- GovernmentJobs.com jobs are real government jobs listed through their platform
UPDATE jobs
SET organization_type = 'local'
WHERE organization_type = 'unknown'
  AND is_active = true
  AND organization ILIKE '%GovernmentJobs%'
  AND (
    description ILIKE '%city%'
    OR description ILIKE '%county%'
    OR description ILIKE '%government%'
    OR description ILIKE '%public sector%'
  );

-- BLOCK 9C: Deactivate known private-sector org blocklist
-- Built from Diagnostic 4 top-40 — pure private sector, no public mission
UPDATE jobs
SET is_active = false
WHERE organization_type = 'unknown'
  AND is_active = true
  AND organization IN (
    -- Travel nursing (no public sector mission)
    'TravelNurseSource',
    'LocumJobsNetwork',
    'LocumJobsOnline',
    'CompHealth',
    'DocGo',
    -- Private security contractors
    'Allied Universal',
    'Allied Universal Inc',
    'Inter-Con Security',
    -- Private finance / consulting
    'EY',
    'Fidelity Investments',
    'Citigroup',
    'SMBC Group',
    'TD Bank',
    'Compass Inc',
    -- Defense contractors (not public sector employers)
    'ClearanceJobs',
    'Clearance Jobs',
    'Booz Allen Hamilton',
    'General Dynamics Information Technology',
    'Leidos',
    'AECOM',
    'Serco',
    'ProSidian Consulting',
    'Genesis10',
    -- Private healthcare systems
    'MedStar Health',
    'Optum',
    'Adventist HealthCare',
    'AMR',
    -- Real estate / other private
    'Equity Lifestyle Properties',
    'InSite Real Estate LLC',
    'Headway',
    'DiversityJobs',
    'DiversityJobs Inc',
    'Confidential'
  );

-- BLOCK 9D: Deactivate by org name pattern — private sector categories
UPDATE jobs
SET is_active = false
WHERE organization_type = 'unknown'
  AND is_active = true
  AND (
    -- Travel nursing patterns
    organization ILIKE '%Travel Nurse%'
    OR organization ILIKE '%TravelNurse%'
    OR organization ILIKE '%Locum%'
    OR organization ILIKE '%Staffing%'
    OR organization ILIKE '%Recruiting%'
    OR organization ILIKE '%Placement%'
    OR organization ILIKE '%Talent%'
    -- Private security
    OR (organization ILIKE '%Security%' AND organization NOT ILIKE '%Social Security%' AND organization NOT ILIKE '%Department of Homeland Security%')
    -- Finance / banking
    OR organization ILIKE '%Bank%'
    OR organization ILIKE '%Financial%'
    OR organization ILIKE '%Investment%'
    OR organization ILIKE '%Capital%'
    OR organization ILIKE '%Insurance%'
    -- Defense contractors (companies, not agencies)
    OR organization ILIKE '%Defense Contractor%'
    OR organization ILIKE '%Government Solutions%'
    OR organization ILIKE '%Government Services%'
    -- Private healthcare staffing (not public hospitals)
    OR organization ILIKE '%Health System%'
    OR organization ILIKE '%Medical Center%'
    OR organization ILIKE '%Hospital%'
    OR organization ILIKE '%Healthcare%'
    -- Real estate
    OR organization ILIKE '%Real Estate%'
    OR organization ILIKE '%Properties%'
    OR organization ILIKE '%Realty%'
  )
  -- Safety net: don't deactivate if the description has strong public-sector signal
  AND NOT (
    description ILIKE '%government%'
    OR description ILIKE '%public sector%'
    OR description ILIKE '%federal agency%'
    OR description ILIKE '%state agency%'
    OR description ILIKE '%city of%'
    OR description ILIKE '%county%'
    OR description ILIKE '%public health department%'
    OR description ILIKE '%public hospital%'
    OR description ILIKE '%veterans%'
    OR description ILIKE '%usajobs%'
  );

-- BLOCK 9E: Deactivate arbeitnow unknowns — this source wasn't covered in pass 8
-- Arbeitnow is a European job board; its "unknown" jobs have no public-sector signal
UPDATE jobs
SET is_active = false
WHERE organization_type = 'unknown'
  AND is_active = true
  AND source = 'arbeitnow'
  AND NOT (
    title ILIKE '%government%'
    OR title ILIKE '%public sector%'
    OR title ILIKE '%public policy%'
    OR title ILIKE '%federal%'
    OR title ILIKE '%municipal%'
    OR title ILIKE '%nonprofit%'
    OR description ILIKE '%government%'
    OR description ILIKE '%public sector%'
    OR description ILIKE '%federal%'
    OR description ILIKE '%city of%'
    OR description ILIKE '%county%'
    OR description ILIKE '%public health department%'
  );

-- BLOCK 9F: Deactivate themuse + remotive unknowns with no public signal
-- These are small sources; unknown jobs here are almost certainly private sector
UPDATE jobs
SET is_active = false
WHERE organization_type = 'unknown'
  AND is_active = true
  AND source IN ('themuse', 'remotive')
  AND NOT (
    title ILIKE '%government%'
    OR title ILIKE '%public sector%'
    OR title ILIKE '%nonprofit%'
    OR title ILIKE '%non-profit%'
    OR description ILIKE '%government%'
    OR description ILIKE '%nonprofit%'
    OR description ILIKE '%501(c)%'
    OR description ILIKE '%public health%'
    OR description ILIKE '%federal agency%'
  );

-- ============================================================
-- FINAL COUNT — target: remaining_unknown < 500
-- ============================================================
SELECT
  COUNT(*) FILTER (WHERE organization_type = 'unknown' AND is_active = true)   AS remaining_unknown,
  COUNT(*) FILTER (WHERE organization_type = 'federal' AND is_active = true)   AS federal,
  COUNT(*) FILTER (WHERE organization_type = 'state' AND is_active = true)     AS state,
  COUNT(*) FILTER (WHERE organization_type = 'local' AND is_active = true)     AS local,
  COUNT(*) FILTER (WHERE organization_type = 'nonprofit' AND is_active = true) AS nonprofit,
  COUNT(*) FILTER (WHERE is_active = true)                                       AS total_active
FROM jobs;

-- Source breakdown of remaining unknowns
SELECT source, COUNT(*) AS remaining_unknown
FROM jobs
WHERE organization_type = 'unknown'
  AND is_active = true
GROUP BY source
ORDER BY remaining_unknown DESC;
