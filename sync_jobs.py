"""
PublicPath — Job Sync Pipeline
================================
Fetches public sector jobs from multiple APIs, normalizes them
to a unified schema, and upserts into Supabase.

Usage:
    python sync_jobs.py                  # Sync all sources
    python sync_jobs.py --source usajobs # Sync one source
    python sync_jobs.py --stats          # Print database stats
    python sync_jobs.py --expire         # Deactivate expired jobs

Setup:
    1. Copy .env.example to .env and fill in your keys
    2. Run 01_create_tables.sql in Supabase SQL Editor
    3. pip install python-dotenv supabase requests
    4. python sync_jobs.py
"""

import argparse
import hashlib
import http.client
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv
from supabase import create_client

# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")  # Use service_role key for writes
USAJOBS_API_KEY = os.getenv("USAJOBS_API_KEY")
USAJOBS_EMAIL = os.getenv("USAJOBS_EMAIL")  # Your email (required by USAJobs)
JOOBLE_API_KEY = os.getenv("JOOBLE_API_KEY")
ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")

# Pilot states — where Harris School students are most likely to work
PILOT_STATES = ["Illinois", "Washington DC", "Virginia", "Maryland", "New York", "California"]

# Public sector search terms for aggregator APIs
PUBLIC_SECTOR_KEYWORDS = [
    "government",
    "public policy",
    "state government",
    "city government",
    "public administration",
    "county government",
    "public health",
    "nonprofit policy",
    "municipal",
]

# US state lookup for normalization
STATE_ABBREVS = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "DC": "District of Columbia", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
    "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine",
    "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska",
    "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico",
    "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas",
    "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
}
STATE_NAME_TO_ABBREV = {v.upper(): k for k, v in STATE_ABBREVS.items()}


# ============================================================
# SUPABASE CLIENT
# ============================================================

def get_supabase():
    """Initialize and return Supabase client."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set in .env")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def parse_location(location_str: str) -> tuple:
    """Parse location string into (city, state_abbrev)."""
    if not location_str:
        return "", ""
    parts = [p.strip() for p in location_str.split(",")]
    city = parts[0] if parts else ""
    state = ""
    for part in parts[1:]:
        part_clean = part.strip().upper()
        if part_clean in STATE_ABBREVS:
            state = part_clean
            break
        if part_clean in STATE_NAME_TO_ABBREV:
            state = STATE_NAME_TO_ABBREV[part_clean]
            break
    return city, state


def parse_salary(salary_str: str) -> tuple:
    """Extract (min, max, basis) from salary string."""
    if not salary_str:
        return None, None, None
    basis = "annual"
    lower = salary_str.lower()
    if "hour" in lower:
        basis = "hourly"
    elif "month" in lower:
        basis = "monthly"
    elif "week" in lower:
        basis = "weekly"
    amounts = re.findall(r'[\$]?([\d,]+\.?\d*)', salary_str)
    amounts = [float(a.replace(',', '')) for a in amounts if a]
    if len(amounts) >= 2:
        return min(amounts), max(amounts), basis
    elif len(amounts) == 1:
        return amounts[0], amounts[0], basis
    return None, None, None


def infer_org_type(text: str) -> str:
    """Infer organization type from text content."""
    text = text.lower()
    federal = ["federal", "u.s. department", "usda", "doj", "dod", "department of defense",
               "department of justice", "department of state", "irs", "fbi", "gsa", "epa",
               "fema", "hhs", "nih", "cdc", "nasa", "va hospital", "usaid"]
    local = ["city of", "county of", "town of", "village of", "borough of", "township", "municipal", "metro"]
    state = ["state of", "state department", "state agency", "commonwealth of"]
    nonprofit = ["nonprofit", "non-profit", "foundation", "ngo", "association", "charity"]
    if any(kw in text for kw in federal):
        return "federal"
    if any(kw in text for kw in local):
        return "local"
    if any(kw in text for kw in state):
        return "state"
    if any(kw in text for kw in nonprofit):
        return "nonprofit"
    return "unknown"


def clean_html(text: str) -> str:
    """Remove HTML tags from text."""
    if not text:
        return ""
    return re.sub(r'<[^>]+>', '', text).strip()


# ============================================================
# USAJOBS FETCHER + NORMALIZER
# ============================================================

def fetch_usajobs(keyword: str = "", location: str = "", page: int = 1, results_per_page: int = 250) -> dict:
    """Fetch jobs from USAJobs Search API."""
    if not USAJOBS_API_KEY or not USAJOBS_EMAIL:
        print("  WARNING: USAJOBS_API_KEY and USAJOBS_EMAIL not set — skipping")
        return {"SearchResult": {"SearchResultItems": [], "SearchResultCount": 0}}

    url = "https://data.usajobs.gov/api/Search"
    headers = {
        "Authorization-Key": USAJOBS_API_KEY,
        "User-Agent": USAJOBS_EMAIL,
        "Host": "data.usajobs.gov",
    }
    params = {
        "Page": page,
        "ResultsPerPage": results_per_page,
    }
    if keyword:
        params["Keyword"] = keyword
    if location:
        params["LocationName"] = location

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def normalize_usajobs(raw_item: dict) -> dict:
    """Map a USAJobs search result item to the unified schema."""
    matched = raw_item.get("MatchedObjectDescriptor", {})
    details = matched.get("UserArea", {}).get("Details", {})
    remuneration = matched.get("PositionRemuneration", [{}])
    rem = remuneration[0] if remuneration else {}
    locations = matched.get("PositionLocation", [{}])
    loc = locations[0] if locations else {}

    # Parse hiring path
    hiring_path_raw = details.get("HiringPath", [])
    if isinstance(hiring_path_raw, str):
        hiring_path_raw = [hiring_path_raw]

    # Determine employment type from schedule
    schedule_list = details.get("PositionSchedule", [])
    emp_type = "full_time"
    if schedule_list:
        sched_name = schedule_list[0].get("Name", "").lower() if isinstance(schedule_list[0], dict) else str(schedule_list[0]).lower()
        if "part" in sched_name:
            emp_type = "part_time"
        elif "intern" in sched_name:
            emp_type = "internship"

    # Check remote/telework
    telework = details.get("TeleworkEligible", False)
    if isinstance(telework, str):
        telework = telework.lower() in ("true", "yes", "1")

    return {
        "source": "usajobs",
        "source_id": matched.get("PositionID", ""),
        "title": matched.get("PositionTitle", ""),
        "organization": matched.get("OrganizationName", ""),
        "organization_type": "federal",
        "description": clean_html(matched.get("QualificationSummary", "")),
        "qualifications": clean_html(matched.get("QualificationSummary", "")),
        "location_city": loc.get("CityName", ""),
        "location_state": loc.get("CountrySubDivisionCode", ""),
        "location_country": "US",
        "is_remote": telework,
        "salary_min": float(rem.get("MinimumRange", 0) or 0) or None,
        "salary_max": float(rem.get("MaximumRange", 0) or 0) or None,
        "salary_basis": rem.get("Description", "Per Year"),
        "pay_grade": f"GS-{details.get('LowGrade', '')}" if details.get("LowGrade") else None,
        "employment_type": emp_type,
        "schedule": details.get("PositionOfferingType", ""),
        "hiring_path": hiring_path_raw if hiring_path_raw else None,
        "application_url": matched.get("ApplyURI", [""])[0] if matched.get("ApplyURI") else "",
        "posted_date": matched.get("PublicationStartDate"),
        "closing_date": matched.get("ApplicationCloseDate"),
        "is_active": True,
        "raw_data": json.dumps(raw_item),  # Store as JSON string for Supabase
    }


def fetch_all_usajobs() -> list:
    """Fetch all current federal jobs, paginating through results."""
    all_normalized = []
    page = 1
    total_pages = 1  # Will be updated after first request

    print(f"  Fetching USAJobs (all current federal listings)...")

    while page <= total_pages:
        try:
            data = fetch_usajobs(page=page, results_per_page=500)
            search_result = data.get("SearchResult", {})
            items = search_result.get("SearchResultItems", [])
            total_count = int(search_result.get("SearchResultCountAll", 0))
            total_pages = min((total_count // 500) + 1, 20)  # Cap at 20 pages = 10,000 jobs

            for item in items:
                try:
                    normalized = normalize_usajobs(item)
                    if normalized["source_id"]:
                        all_normalized.append(normalized)
                except Exception as e:
                    print(f"    Normalize error: {e}")

            print(f"    Page {page}/{total_pages}: got {len(items)} jobs (total so far: {len(all_normalized)})")
            page += 1
            time.sleep(1)  # Be polite to USAJobs API

        except Exception as e:
            print(f"    Fetch error on page {page}: {e}")
            break

    return all_normalized


# ============================================================
# JOOBLE FETCHER + NORMALIZER
# ============================================================

def fetch_jooble(keywords: str, location: str, page: int = 1) -> list:
    """Fetch jobs from Jooble API using their official http.client pattern."""
    if not JOOBLE_API_KEY:
        print("  WARNING: JOOBLE_API_KEY not set — skipping")
        return []

    connection = http.client.HTTPSConnection("jooble.org")
    headers = {"Content-type": "application/json"}
    body = json.dumps({
        "keywords": keywords,
        "location": location,
        "page": str(page),
    })

    connection.request("POST", "/api/" + JOOBLE_API_KEY, body, headers)
    response = connection.getresponse()

    if response.status != 200:
        print(f"    Jooble error: {response.status} {response.reason}")
        return []

    raw = response.read().decode("utf-8")
    data = json.loads(raw)
    connection.close()

    return data.get("jobs", [])


def normalize_jooble(raw: dict) -> dict:
    """Map a Jooble job to the unified schema."""
    city, state = parse_location(raw.get("location", ""))
    sal_min, sal_max, sal_basis = parse_salary(raw.get("salary", ""))
    title = raw.get("title", "")
    company = raw.get("company", "")
    snippet = clean_html(raw.get("snippet", ""))
    combined_text = f"{company} {title} {snippet}"

    # Generate stable ID
    source_id = str(raw.get("id", ""))
    if not source_id:
        source_id = hashlib.md5(raw.get("link", "").encode()).hexdigest()

    return {
        "source": "jooble",
        "source_id": source_id,
        "title": title,
        "organization": company,
        "organization_type": infer_org_type(combined_text),
        "description": snippet,
        "location_city": city,
        "location_state": state,
        "location_country": "US",
        "is_remote": "remote" in title.lower() or "remote" in raw.get("location", "").lower(),
        "salary_min": sal_min,
        "salary_max": sal_max,
        "salary_basis": sal_basis,
        "employment_type": raw.get("type", "full_time"),
        "application_url": raw.get("link", ""),
        "posted_date": raw.get("updated"),
        "is_active": True,
        "raw_data": json.dumps(raw),
    }


def fetch_all_jooble(max_pages_per_query: int = 2) -> list:
    """Fetch public sector jobs from Jooble across all keyword/location combos."""
    all_jobs = {}  # Deduplicate by link URL
    total_queries = len(PUBLIC_SECTOR_KEYWORDS) * len(PILOT_STATES)
    query_num = 0

    for keyword in PUBLIC_SECTOR_KEYWORDS:
        for location in PILOT_STATES:
            query_num += 1
            print(f"    [{query_num}/{total_queries}] '{keyword}' in {location}")

            for page in range(1, max_pages_per_query + 1):
                try:
                    jobs = fetch_jooble(keyword, location, page=page)
                    if not jobs:
                        break

                    for job in jobs:
                        link = job.get("link", "")
                        if link and link not in all_jobs:
                            normalized = normalize_jooble(job)
                            if normalized["source_id"]:
                                all_jobs[link] = normalized

                    time.sleep(0.5)  # Rate limit protection

                except Exception as e:
                    print(f"      Error: {e}")
                    break

    result = list(all_jobs.values())
    print(f"    Total unique Jooble jobs: {len(result)}")
    return result


# ============================================================
# ADZUNA FETCHER + NORMALIZER
# ============================================================

def fetch_adzuna(keyword: str, location: str, page: int = 1, results_per_page: int = 50) -> list:
    """Fetch jobs from Adzuna API. Uses country code 'us'."""
    if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
        print("  WARNING: ADZUNA_APP_ID / ADZUNA_APP_KEY not set — skipping")
        return []

    url = f"https://api.adzuna.com/v1/api/jobs/us/search/{page}"
    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "what": keyword,
        "where": location,
        "results_per_page": results_per_page,
        "content-type": "application/json",
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    return data.get("results", [])


def normalize_adzuna(raw: dict) -> dict:
    """Map an Adzuna job result to the unified schema."""
    location_obj = raw.get("location", {})
    area = location_obj.get("area", [])
    display = location_obj.get("display_name", "")
    city, state = parse_location(display)

    # If parse_location didn't find a state, try the area array
    if not state and len(area) >= 2:
        for part in area[1:]:
            upper = part.strip().upper()
            if upper in STATE_ABBREVS:
                state = upper
                break
            if upper in STATE_NAME_TO_ABBREV:
                state = STATE_NAME_TO_ABBREV[upper]
                break

    company = raw.get("company", {}).get("display_name", "")
    category = raw.get("category", {})
    title = raw.get("title", "")
    description = clean_html(raw.get("description", ""))
    combined_text = f"{company} {title} {description}"

    # Contract type mapping
    contract_type = raw.get("contract_type", "")
    contract_time = raw.get("contract_time", "")
    emp_type = "full_time"
    if contract_time == "part_time":
        emp_type = "part_time"
    elif "intern" in title.lower():
        emp_type = "internship"

    return {
        "source": "adzuna",
        "source_id": str(raw.get("id", "")),
        "title": clean_html(title),
        "organization": company,
        "organization_type": infer_org_type(combined_text),
        "description": description,
        "location_city": city,
        "location_state": state,
        "location_country": "US",
        "is_remote": "remote" in title.lower() or "remote" in display.lower(),
        "salary_min": raw.get("salary_min"),
        "salary_max": raw.get("salary_max"),
        "salary_basis": "annual",
        "job_category": category.get("label", ""),
        "employment_type": emp_type,
        "schedule": contract_type if contract_type else None,
        "application_url": raw.get("redirect_url", ""),
        "posted_date": raw.get("created"),
        "is_active": True,
        "raw_data": json.dumps(raw),
    }


def fetch_all_adzuna(max_pages_per_query: int = 2) -> list:
    """Fetch public sector jobs from Adzuna across keyword/location combos."""
    all_jobs = {}  # Deduplicate by Adzuna job ID
    total_queries = len(PUBLIC_SECTOR_KEYWORDS) * len(PILOT_STATES)
    query_num = 0

    for keyword in PUBLIC_SECTOR_KEYWORDS:
        for location in PILOT_STATES:
            query_num += 1
            print(f"    [{query_num}/{total_queries}] '{keyword}' in {location}")

            for page in range(1, max_pages_per_query + 1):
                try:
                    jobs = fetch_adzuna(keyword, location, page=page)
                    if not jobs:
                        break

                    for job in jobs:
                        job_id = str(job.get("id", ""))
                        if job_id and job_id not in all_jobs:
                            normalized = normalize_adzuna(job)
                            if normalized["source_id"]:
                                all_jobs[job_id] = normalized

                    time.sleep(0.3)  # Adzuna rate limit

                except Exception as e:
                    print(f"      Error: {e}")
                    break

    result = list(all_jobs.values())
    print(f"    Total unique Adzuna jobs: {len(result)}")
    return result


# ============================================================
# SYNC ENGINE
# ============================================================

def upsert_jobs(supabase, jobs: list, source_name: str, batch_size: int = 50) -> dict:
    """Upsert normalized jobs into Supabase in batches."""
    stats = {"inserted": 0, "errors": 0, "error_messages": []}

    # Clean None values — Supabase handles defaults
    cleaned = []
    for job in jobs:
        clean = {k: v for k, v in job.items() if v is not None}
        cleaned.append(clean)

    for i in range(0, len(cleaned), batch_size):
        batch = cleaned[i:i + batch_size]
        try:
            supabase.table("jobs").upsert(
                batch,
                on_conflict="source,source_id"
            ).execute()
            stats["inserted"] += len(batch)
        except Exception as e:
            stats["errors"] += len(batch)
            stats["error_messages"].append(str(e))
            print(f"    Upsert error (batch {i//batch_size + 1}): {e}")

    return stats


def sync_source(supabase, source_name: str, fetch_fn, **kwargs):
    """Sync a single source: fetch, normalize, upsert, log."""
    print(f"\n{'='*60}")
    print(f"SYNCING: {source_name.upper()}")
    print(f"{'='*60}")

    log_entry = {
        "source": source_name,
        "started_at": datetime.utcnow().isoformat(),
        "jobs_fetched": 0,
        "jobs_new": 0,
        "jobs_updated": 0,
        "errors": [],
    }

    try:
        # Fetch and normalize
        normalized_jobs = fetch_fn(**kwargs)
        log_entry["jobs_fetched"] = len(normalized_jobs)
        print(f"  Fetched {len(normalized_jobs)} jobs")

        if not normalized_jobs:
            print("  No jobs to upsert")
        else:
            # Upsert to Supabase
            stats = upsert_jobs(supabase, normalized_jobs, source_name)
            log_entry["jobs_new"] = stats["inserted"]
            log_entry["errors"] = stats["error_messages"]
            print(f"  Upserted: {stats['inserted']}, Errors: {stats['errors']}")

    except Exception as e:
        log_entry["errors"] = [str(e)]
        print(f"  FATAL ERROR: {e}")

    log_entry["completed_at"] = datetime.utcnow().isoformat()

    # Write sync log
    try:
        supabase.table("sync_log").insert(log_entry).execute()
    except Exception as e:
        print(f"  Could not write sync log: {e}")

    return log_entry


def expire_old_jobs(supabase):
    """Mark jobs past their closing date as inactive."""
    print(f"\n{'='*60}")
    print("EXPIRING OLD JOBS")
    print(f"{'='*60}")
    try:
        result = supabase.rpc("deactivate_expired_jobs").execute()
        count = result.data if result.data else 0
        print(f"  Deactivated {count} expired jobs")
    except Exception as e:
        print(f"  Error: {e}")


def print_stats(supabase):
    """Print database summary stats."""
    print(f"\n{'='*60}")
    print("DATABASE STATS")
    print(f"{'='*60}")
    try:
        result = supabase.rpc("get_job_stats").execute()
        stats = result.data
        if stats:
            print(f"  Total jobs:           {stats.get('total_jobs', 0)}")
            print(f"  Active jobs:          {stats.get('active_jobs', 0)}")
            print(f"  Closing in 7 days:    {stats.get('closing_within_7_days', 0)}")
            print(f"\n  By Source:")
            for source, count in (stats.get("by_source") or {}).items():
                print(f"    {source}: {count}")
            print(f"\n  By Org Type:")
            for org, count in (stats.get("by_org_type") or {}).items():
                print(f"    {org}: {count}")
            print(f"\n  By State (top 15):")
            for state, count in sorted((stats.get("by_state") or {}).items(), key=lambda x: -x[1]):
                print(f"    {state}: {count}")
            last = stats.get("last_sync")
            if last:
                print(f"\n  Last sync: {last.get('source')} at {last.get('completed_at')} ({last.get('jobs_fetched')} jobs)")
    except Exception as e:
        print(f"  Error fetching stats: {e}")
        print("  (Have you run 01_create_tables.sql in Supabase yet?)")


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="PublicPath Job Sync Pipeline")
    parser.add_argument("--source", choices=["usajobs", "jooble", "adzuna", "all"], default="all",
                        help="Which source to sync (default: all)")
    parser.add_argument("--stats", action="store_true",
                        help="Print database stats and exit")
    parser.add_argument("--expire", action="store_true",
                        help="Deactivate expired jobs and exit")
    parser.add_argument("--jooble-pages", type=int, default=2,
                        help="Max pages per Jooble query (default: 2)")
    args = parser.parse_args()

    supabase = get_supabase()

    if args.stats:
        print_stats(supabase)
        return

    if args.expire:
        expire_old_jobs(supabase)
        return

    print("=" * 60)
    print(f"PublicPath Job Sync — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    # Sync sources
    if args.source in ("usajobs", "all"):
        sync_source(supabase, "usajobs", fetch_all_usajobs)

    if args.source in ("jooble", "all"):
        sync_source(supabase, "jooble", fetch_all_jooble, max_pages_per_query=args.jooble_pages)

    if args.source in ("adzuna", "all"):
        sync_source(supabase, "adzuna", fetch_all_adzuna)

    # Expire old jobs after syncing
    expire_old_jobs(supabase)

   # Classify experience levels and normalize employment types for new jobs
    try:
        supabase.rpc("classify_experience_levels").execute()
        print("  Classified experience levels for new jobs")
    except Exception as e:
        print(f"  Error classifying experience levels: {e}")

    try:
        supabase.rpc("normalize_employment_types").execute()
        print("  Normalized employment types")
    except Exception as e:
        print(f"  Error normalizing employment types: {e}") 

    # Print summary
    print_stats(supabase)

    print(f"\n{'='*60}")
    print("SYNC COMPLETE")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
