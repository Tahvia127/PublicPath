-- ============================================================
-- PublicPath — Sector categories + entry-level classification
-- Run once in Supabase SQL Editor, then call the RPCs below.
-- ============================================================

-- 1. Add new columns (safe: IF NOT EXISTS)
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS sector TEXT;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS is_entry_level BOOLEAN;

-- 2. Indexes for fast filtering
CREATE INDEX IF NOT EXISTS idx_jobs_sector      ON jobs(sector)        WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_jobs_entry_level ON jobs(is_entry_level) WHERE is_active = true;

-- ============================================================
-- 3. classify_job_sectors()
--    Assigns a sector to every active job based on title keywords.
--    Priority order: specific sectors before catch-all "government".
--    Safe to re-run — only touches NULL rows.
-- ============================================================
CREATE OR REPLACE FUNCTION classify_job_sectors()
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE updated INTEGER;
BEGIN
  -- Reset to NULL so function is idempotent when called with force
  -- (comment this out if you only want to fill NULLs)
  -- UPDATE jobs SET sector = NULL WHERE is_active = true;

  -- Politics & Campaigns
  UPDATE jobs SET sector = 'politics'
  WHERE is_active = true AND sector IS NULL
    AND (
      lower(title) ~ '(campaign|field organizer|canvass|voter registration|political director|advance staff|gotv|get out the vote|precinct captain|ballot initiative|political operative|political organizing)'
      OR lower(organization) ~ '(democratic|republican|campaign committee|pac$| pac | dnc | rnc |political action|party committee)'
    );

  -- Technology & Digital Services
  UPDATE jobs SET sector = 'technology'
  WHERE is_active = true AND sector IS NULL
    AND lower(title) ~ '(software engineer|software developer|web developer|front.?end|back.?end|full.?stack|data scientist|data engineer|machine learning|cybersecurity|information security|it specialist|it manager|systems administrator|network admin|database admin|cloud engineer|devops|ux designer|product manager|digital service|technology officer|cto|cio|sre )';

  -- Public Health & Healthcare
  UPDATE jobs SET sector = 'public_health'
  WHERE is_active = true AND sector IS NULL
    AND lower(title) ~ '(epidemiologist|public health|health educator|health inspector|registered nurse|clinical nurse|nutritionist|mental health|behavioral health|substance abuse|disease investigator|medical officer|dental|physician|pharmacist|health analyst|health specialist)';

  -- Social Services & Human Services
  UPDATE jobs SET sector = 'social_services'
  WHERE is_active = true AND sector IS NULL
    AND lower(title) ~ '(social worker|case manager|caseworker|child welfare|family services|benefits specialist|workforce development|navigator|housing specialist|youth worker|human services|community health worker|employment specialist|social services specialist|benefits counselor)';

  -- Education & Training
  UPDATE jobs SET sector = 'education'
  WHERE is_active = true AND sector IS NULL
    AND lower(title) ~ '(teacher|librarian|curriculum|instruction specialist|school counselor|academic advisor|education coordinator|education specialist|training specialist|school psychologist|early childhood|special education|teaching assistant)';

  -- Environment, Conservation & Infrastructure
  UPDATE jobs SET sector = 'environment'
  WHERE is_active = true AND sector IS NULL
    AND lower(title) ~ '(environmental|conservation|sustainability|natural resources|wildlife|forestry|parks ranger|climate|clean energy|water quality|urban planner|city planner|civil engineer|transportation planner|infrastructure|land management)';

  -- Legal, Compliance & Law Enforcement
  UPDATE jobs SET sector = 'legal'
  WHERE is_active = true AND sector IS NULL
    AND lower(title) ~ '(attorney|paralegal|legal counsel|compliance officer|inspector general|law clerk|judge|corrections officer|probation officer|parole officer|police officer|detective|deputy sheriff|border patrol|customs agent|us marshal|special agent|criminal investigator)';

  -- Finance, Budget & Economics
  UPDATE jobs SET sector = 'finance'
  WHERE is_active = true AND sector IS NULL
    AND lower(title) ~ '(accountant|financial analyst|budget analyst|fiscal analyst|grants manager|procurement specialist|contracting officer|economist|revenue analyst|tax examiner|tax specialist|auditor|treasury|financial specialist|grants administrator)';

  -- Communications & Public Affairs
  UPDATE jobs SET sector = 'communications'
  WHERE is_active = true AND sector IS NULL
    AND lower(title) ~ '(communications specialist|communications director|public affairs|media relations|press secretary|content writer|editor|graphic designer|social media|outreach coordinator|public information officer|communications officer|digital communications)';

  -- Policy, Research & Legislative Affairs
  UPDATE jobs SET sector = 'policy'
  WHERE is_active = true AND sector IS NULL
    AND lower(title) ~ '(policy analyst|policy advisor|legislative assistant|regulatory analyst|program analyst|research analyst|policy associate|policy specialist|policy coordinator|policy director|policy fellow|research associate|government relations|legislative liaison)';

  -- Default catch-all: General Government & Public Administration
  UPDATE jobs SET sector = 'government'
  WHERE is_active = true AND sector IS NULL;

  SELECT COUNT(*) INTO updated FROM jobs WHERE is_active = true AND sector IS NOT NULL;
  RETURN updated;
END;
$$;

-- ============================================================
-- 4. classify_entry_levels()
--    Sets is_entry_level = TRUE/FALSE on active jobs.
--    Uses GS grade, title keywords, and salary range.
--    Safe to re-run — only touches NULL rows.
-- ============================================================
CREATE OR REPLACE FUNCTION classify_entry_levels()
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE updated INTEGER;
BEGIN
  -- Internships = always entry level
  UPDATE jobs SET is_entry_level = true
  WHERE is_active = true AND is_entry_level IS NULL
    AND (employment_type = 'internship' OR lower(title) ~ '(intern|internship)');

  -- GS-4 through GS-9 = federal entry level
  UPDATE jobs SET is_entry_level = true
  WHERE is_active = true AND is_entry_level IS NULL
    AND pay_grade ~ '^GS-[4-9]$';

  -- GS-10 through GS-15 = federal mid/senior
  UPDATE jobs SET is_entry_level = false
  WHERE is_active = true AND is_entry_level IS NULL
    AND pay_grade ~ '^GS-(1[0-5])$';

  -- Entry-level title keywords
  UPDATE jobs SET is_entry_level = true
  WHERE is_active = true AND is_entry_level IS NULL
    AND lower(title) ~ '(entry.?level|entry level|junior |associate |trainee| i$| i | analyst i | specialist i |officer i |recent graduate|early career| fellow | fellowship |pathways|summer associate)';

  -- Clearly senior titles = NOT entry level
  UPDATE jobs SET is_entry_level = false
  WHERE is_active = true AND is_entry_level IS NULL
    AND lower(title) ~ '(senior |sr\. | sr | director| manager|chief |vice president|executive director|principal |lead |head of |superintendent|commissioner|deputy director|managing director|president|deputy secretary)';

  -- Salary under $72k/year with no other signal = likely entry
  UPDATE jobs SET is_entry_level = true
  WHERE is_active = true AND is_entry_level IS NULL
    AND salary_max IS NOT NULL AND salary_max < 72000 AND salary_basis = 'annual';

  -- Salary over $120k with no grade info = likely not entry
  UPDATE jobs SET is_entry_level = false
  WHERE is_active = true AND is_entry_level IS NULL
    AND salary_max IS NOT NULL AND salary_max > 120000;

  -- Remaining unknowns: default true for public sector
  -- (most public sector jobs without seniority signals are accessible entry-mid)
  UPDATE jobs SET is_entry_level = true
  WHERE is_active = true AND is_entry_level IS NULL;

  SELECT COUNT(*) INTO updated FROM jobs WHERE is_active = true AND is_entry_level = true;
  RETURN updated;
END;
$$;

-- ============================================================
-- 5. Run immediately after creating the functions
-- ============================================================
SELECT classify_job_sectors();
SELECT classify_entry_levels();
