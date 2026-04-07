-- ============================================================
-- cleanup_non_us_jobs() RPC function
-- Deactivates jobs that are clearly not US-based
-- Run in Supabase SQL Editor
-- ============================================================

CREATE OR REPLACE FUNCTION cleanup_non_us_jobs()
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  deactivated_count INTEGER := 0;
BEGIN
  -- Deactivate jobs where location_country is explicitly non-US
  UPDATE jobs
  SET is_active = false
  WHERE is_active = true
    AND location_country IS NOT NULL
    AND location_country != ''
    AND UPPER(location_country) NOT IN ('US', 'USA', 'UNITED STATES', 'UNITED STATES OF AMERICA');

  GET DIAGNOSTICS deactivated_count = ROW_COUNT;

  -- Also deactivate jobs with non-US city/state patterns
  UPDATE jobs
  SET is_active = false
  WHERE is_active = true
    AND (
      -- UK locations
      location_city ILIKE '%london%'
      OR location_city ILIKE '%manchester%'
      OR location_city ILIKE '%birmingham%'
      OR location_city ILIKE '%edinburgh%'
      OR location_city ILIKE '%glasgow%'
      -- Canadian locations
      OR location_city ILIKE '%toronto%'
      OR location_city ILIKE '%vancouver%'
      OR location_city ILIKE '%montreal%'
      OR location_city ILIKE '%ottawa%'
      OR location_city ILIKE '%calgary%'
      -- Australian locations
      OR location_city ILIKE '%sydney%'
      OR location_city ILIKE '%melbourne%'
      OR location_city ILIKE '%brisbane%'
      -- European locations
      OR location_city ILIKE '%berlin%'
      OR location_city ILIKE '%paris%'
      OR location_city ILIKE '%amsterdam%'
      OR location_city ILIKE '%dublin%'
    )
    AND (location_state IS NULL OR location_state = '');

  deactivated_count := deactivated_count + (SELECT COUNT(*) FROM jobs WHERE is_active = false AND updated_at >= NOW() - INTERVAL '1 second');

  RETURN deactivated_count;
END;
$$;

-- Grant execute permission
GRANT EXECUTE ON FUNCTION cleanup_non_us_jobs() TO service_role;
GRANT EXECUTE ON FUNCTION cleanup_non_us_jobs() TO authenticated;

-- Run the cleanup
SELECT cleanup_non_us_jobs() AS deactivated_non_us_jobs;

-- Verify: count remaining active jobs by country
SELECT
  COALESCE(UPPER(location_country), 'NULL/EMPTY') AS country,
  COUNT(*) AS job_count
FROM jobs
WHERE is_active = true
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20;
