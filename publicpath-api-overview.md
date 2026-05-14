# PublicPath – Jobs API Details (Supabase / Filters Overview)

**To:** bard.luippold@gmail.com
**Subject:** PublicPath – Jobs API details (Supabase / filters overview)

---

Hi Bard,

Sharing the technical details on the PublicPath jobs data layer so you have what you need to advise on the filtering path forward.

## Stack

PublicPath uses **Supabase** (Postgres + REST API). The front end calls it directly via the Supabase JS SDK using a publishable anon key.

## Base Query (every page load)

```js
supabase
  .from('jobs')
  .select('*', { count: 'exact' })
  .eq('is_active', true)
```

This fetches all active jobs. Pagination is 25 per page using `.range(from, from + 24)`.

## Database Columns Returned (full row)

| Column | Type | Notes |
|---|---|---|
| `id` | uuid | |
| `title` | text | |
| `organization` | text | |
| `organization_type` | text | `federal`, `state`, `local`, `unknown` |
| `sector` | text | see sector list below |
| `location_city` | text | |
| `location_state` | text | full name or abbrev (e.g. "Illinois" or "IL") |
| `employment_type` | text | `full_time`, `part_time`, `internship` |
| `experience_level` | text | `entry`, `mid`, `senior` |
| `is_entry_level` | boolean | |
| `is_remote` | boolean | |
| `is_active` | boolean | always `true` in queries |
| `salary_min` | numeric | |
| `salary_max` | numeric | |
| `pay_grade` | text | e.g. `GS-7` |
| `closing_date` | timestamptz | |
| `posted_date` | timestamptz | |
| `application_url` | text | |
| `description` | text | |

## Sector Values

`government`, `policy`, `politics`, `technology`, `social_services`, `public_health`, `education`, `environment`, `legal`, `finance`, `communications`

## Filters Currently Implemented (server-side via Supabase query)

| Filter | Mechanism |
|---|---|
| Text search | `title.ilike.%q%` OR `organization.ilike.%q%` |
| Sector | exact `eq('sector', value)` |
| State | `location_state.eq.{variant}` across abbrev + full name |
| Org type (federal/state/local) | `in('organization_type', [...])` |
| Fellowship | title keyword match (`.ilike.%fellow%`, `%PMF%`, etc.) |
| Employment type | `in('employment_type', [variants])` |
| Experience level | OR on `is_entry_level`, `employment_type`, title keywords |
| Remote only | `is('is_remote', true)` |
| Closing within 7 days | `closing_date` between now and +7 days |
| Sort | `posted_date` DESC / `closing_date` ASC / `salary_max` DESC |

## On the RegEx / Front-End Filtering Approach

Since `select('*')` returns the full row, all fields are available client-side immediately after the fetch. RegEx on `title`, `organization`, `description`, `location_city`, or `location_state` would work cleanly for any filter that doesn't need to reduce the result count (e.g. display-only filtering within a loaded page). For filters that need accurate counts or span multiple pages, pushing the filter up to the Supabase query (as it's done today) is more reliable.

Happy to hop on a call or share the `jobs.html` source directly if that's easier.

— Tahvia / PublicPath
