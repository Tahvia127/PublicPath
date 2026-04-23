-- ============================================================
-- PublicPath — Row Level Security for subscribers table
-- Run once in Supabase SQL Editor.
-- Prevents any client-side SELECT on subscriber PII.
-- ============================================================

-- 1. Enable RLS on the subscribers table
ALTER TABLE subscribers ENABLE ROW LEVEL SECURITY;

-- 2. DROP any existing policies so this script is idempotent
DROP POLICY IF EXISTS "anon_insert_only"  ON subscribers;
DROP POLICY IF EXISTS "service_role_all"  ON subscribers;

-- 3. Anonymous users (website visitors) may only INSERT — never SELECT
--    This means email addresses are never readable from the browser.
CREATE POLICY "anon_insert_only" ON subscribers
  FOR INSERT
  TO anon
  WITH CHECK (true);

-- 4. Service role (your backend / GitHub Actions) retains full access
CREATE POLICY "service_role_all" ON subscribers
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

-- ============================================================
-- 5. Verify: the jobs table should never expose subscriber data.
--    Confirm jobs has NO email or PII columns:
-- ============================================================
-- SELECT column_name FROM information_schema.columns
-- WHERE table_name = 'jobs'
-- ORDER BY ordinal_position;
