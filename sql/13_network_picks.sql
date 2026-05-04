-- ============================================================
-- PublicPath — network_picks table
-- Stores roles flagged from personal network, listservs, or
-- direct referrals for inclusion in the weekly digest email.
-- Run once in Supabase SQL Editor.
-- ============================================================

CREATE TABLE IF NOT EXISTS network_picks (
  id            uuid DEFAULT gen_random_uuid() PRIMARY KEY,

  -- Role details
  title         text        NOT NULL,
  agency_org    text        NOT NULL,
  location      text,
  url           text,
  closing_date  date,
  salary_range  text,                       -- free-text, e.g. "$55k–$75k"

  -- Curation metadata
  notes         text,                       -- 1-line note on why it's worth looking at
  source_note   text,                       -- e.g. "Flagged by a former HHS staffer"
  digest_week   date        NOT NULL,       -- Monday of the week it should be included
  included      boolean     DEFAULT false,  -- toggle to true once placed in a sent digest

  -- Timestamps
  created_at    timestamptz DEFAULT now(),
  updated_at    timestamptz DEFAULT now()
);

-- Index for quick lookup by digest week
CREATE INDEX IF NOT EXISTS idx_network_picks_digest_week
  ON network_picks (digest_week DESC);

-- Auto-update updated_at on row change
CREATE OR REPLACE FUNCTION update_network_picks_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_network_picks_updated_at ON network_picks;
CREATE TRIGGER trg_network_picks_updated_at
  BEFORE UPDATE ON network_picks
  FOR EACH ROW EXECUTE FUNCTION update_network_picks_updated_at();

-- ============================================================
-- Row Level Security
-- Visitors can never read this table (it contains sourcing PII).
-- Only the service role (backend / admin) has full access.
-- ============================================================

ALTER TABLE network_picks ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "service_role_all" ON network_picks;
CREATE POLICY "service_role_all" ON network_picks
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

-- ============================================================
-- Useful admin queries (run in Supabase Table Editor or SQL Editor)
-- ============================================================

-- View this week's picks:
-- SELECT title, agency_org, location, source_note, url
-- FROM network_picks
-- WHERE digest_week = date_trunc('week', current_date)::date
-- ORDER BY created_at;

-- Mark a pick as included after sending:
-- UPDATE network_picks SET included = true WHERE id = '<uuid>';
