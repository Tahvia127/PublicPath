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
"""

import argparse
import hashlib
import http.client
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
USAJOBS_API_KEY = os.getenv("USAJOBS_API_KEY")
USAJOBS_EMAIL = os.getenv("USAJOBS_EMAIL")
JOOBLE_API_KEY = os.getenv("JOOBLE_API_KEY")
ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")
SERPAPI_KEY = os.getenv("SERPAPI_KEY")
FINDWORK_API_KEY = os.getenv("FINDWORK_API_KEY")

PILOT_STATES = ["Illinois", "Washington DC", "Virginia", "Maryland", "New York", "California"]

# Organizations permanently blocked from ingestion — confirmed private sector
ORG_BLOCKLIST = {
    # Travel nursing / locum staffing
    "travelnursesource", "locumjobsnetwork", "locumjobsonline", "comphealth",
    "docgo", "aya healthcare", "host healthcare", "lancesoft", "atlas medstaff",
    "ahs staffing", "onestaff medical", "talent4health", "care career",
    "malone healthcare",
    # Private security
    "allied universal", "allied universal inc", "inter-con security",
    # Finance / banking / consulting
    "ey", "fidelity investments", "citigroup", "smbc group", "td bank",
    "goldman sachs", "morgan stanley", "jpmorgan", "blackrock", "kpmg",
    "pwc", "bcg", "mckinsey", "bain", "deloitte",
    # Defense contractors (not government employers)
    "clearancejobs", "clearance jobs", "booz allen hamilton",
    "general dynamics information technology", "leidos", "aecom", "serco",
    "prosidian consulting", "genesis10", "koniag government services",
    "sosi", "venesco", "ripple effect", "alert it solutions",
    # Private healthcare / real estate / other
    "medstar health", "optum", "adventist healthcare", "amr",
    "equity lifestyle properties", "insite real estate llc", "headway",
    "diversityjobs", "diversityjobs inc", "compass inc", "sunbit",
    "tylin", "congruex", "general motors",
}

PUBLIC_SECTOR_KEYWORDS = [
    "government", "public policy", "state government", "city government",
    "public administration", "county government", "public health",
    "nonprofit policy", "municipal",
]

PUBLIC_SECTOR_TERMS = [
    # Government / federal
    "government", "federal government", "federal agency", "federal employee",
    "u.s. department", "us department", "united states department",
    "department of ", "bureau of ", "office of ",
    "usajobs", "general schedule", "gs-", "competitive service",
    # State / local
    "state government", "state agency", "state of ", "commonwealth of ",
    "city of ", "county of ", "town of ", "village of ",
    "municipal", "public works", "transit authority", "school district",
    # Civic / nonprofit
    "nonprofit", "non-profit", "501(c)", "public interest", "civic",
    "public administration", "public policy", "public health", "public sector",
    # Legislative / judicial
    "legislature", "legislative", "judicial", "city council", "county board",
]

# ── SECTOR CLASSIFICATION ──────────────────────────────────────
# Priority order matters: match on title (reliable); desc can be noisy.
SECTOR_KEYWORDS = {
    'politics':       ['campaign ', 'field organizer', 'canvass', 'voter registration',
                       'political director', 'advance staff', 'gotv', 'get out the vote',
                       'precinct captain', 'political operative', 'political organizing',
                       'ballot initiative'],
    'technology':     ['software engineer', 'software developer', 'web developer',
                       'data scientist', 'data engineer', 'machine learning', 'cybersecurity',
                       'information security', 'it specialist', 'systems administrator',
                       'network admin', 'database admin', 'cloud engineer', 'devops',
                       'ux designer', 'product manager', 'digital service', 'technology officer'],
    'social_services':['social worker', 'case manager', 'caseworker', 'child welfare',
                       'family services', 'benefits specialist', 'workforce development',
                       'navigator', 'housing specialist', 'youth worker', 'human services',
                       'community health worker', 'employment specialist'],
    'public_health':  ['epidemiologist', 'public health', 'health educator', 'health inspector',
                       'registered nurse', 'clinical nurse', 'nutritionist', 'mental health',
                       'behavioral health', 'substance abuse', 'disease investigator',
                       'medical officer', 'health analyst'],
    'education':      ['teacher', 'librarian', 'curriculum', 'instruction specialist',
                       'school counselor', 'academic advisor', 'education coordinator',
                       'education specialist', 'training specialist', 'school psychologist',
                       'early childhood', 'special education'],
    'environment':    ['environmental', 'conservation', 'sustainability', 'natural resources',
                       'wildlife', 'forestry', 'parks ranger', 'climate', 'clean energy',
                       'water quality', 'urban planner', 'city planner', 'civil engineer',
                       'transportation planner', 'land management'],
    'legal':          ['attorney', 'paralegal', 'legal counsel', 'compliance officer',
                       'inspector general', 'law clerk', 'judge', 'corrections officer',
                       'probation officer', 'parole officer', 'police officer', 'detective',
                       'deputy sheriff', 'border patrol', 'customs agent', 'us marshal',
                       'special agent', 'criminal investigator'],
    'finance':        ['accountant', 'financial analyst', 'budget analyst', 'fiscal analyst',
                       'grants manager', 'procurement specialist', 'contracting officer',
                       'economist', 'revenue analyst', 'tax examiner', 'auditor', 'treasury',
                       'grants administrator'],
    'communications': ['communications specialist', 'communications director', 'public affairs',
                       'media relations', 'press secretary', 'content writer', 'editor',
                       'graphic designer', 'social media', 'outreach coordinator',
                       'public information officer', 'digital communications'],
    'policy':         ['policy analyst', 'policy advisor', 'legislative assistant',
                       'regulatory analyst', 'program analyst', 'research analyst',
                       'policy associate', 'policy specialist', 'policy coordinator',
                       'policy director', 'policy fellow', 'research associate',
                       'government relations', 'legislative liaison'],
    # 'government' is the catch-all — no keywords needed
}

ENTRY_LEVEL_TITLE_KW = [
    'entry level', 'entry-level', 'junior ', 'associate ', 'trainee', 'intern',
    'assistant ', 'coordinator', 'analyst i', 'specialist i', 'officer i',
    'recent graduate', 'early career', ' fellow', 'fellowship', 'pathways',
    'summer associate',
]

SENIOR_TITLE_KW = [
    'senior ', 'sr. ', ' sr ', 'director', ' manager', 'chief ', 'vice president',
    'executive director', 'principal ', 'lead ', 'head of', 'superintendent',
    'commissioner', 'deputy director', 'managing director', 'president',
    'deputy secretary',
]


def infer_sector(title, description=""):
    """Classify a job into a sector based on title keywords."""
    text = title.lower()
    priority = [
        'politics', 'technology', 'social_services', 'public_health',
        'education', 'environment', 'legal', 'finance', 'communications', 'policy',
    ]
    for sector in priority:
        if any(kw in text for kw in SECTOR_KEYWORDS[sector]):
            return sector
    return 'government'


def infer_entry_level(title, pay_grade=None, salary_max=None, employment_type=None, description=""):
    """Return True if entry-level, False if senior, None if unclear."""
    t = title.lower()

    # Internships are always entry level
    if employment_type == 'internship' or 'intern' in t:
        return True

    # Clear senior signals
    if any(kw in t for kw in SENIOR_TITLE_KW):
        return False

    # Federal GS grades
    if pay_grade:
        m = re.search(r'GS-(\d+)', str(pay_grade))
        if m:
            grade = int(m.group(1))
            if grade <= 9:
                return True
            if grade >= 12:
                return False

    # Entry-level title keywords
    if any(kw in t for kw in ENTRY_LEVEL_TITLE_KW):
        return True

    # Salary signal
    if salary_max:
        if salary_max < 70000:
            return True
        if salary_max > 120000:
            return False

    return None  # unknown; SQL classify_entry_levels() handles residuals


def enrich_job(job):
    """Add sector and is_entry_level to a normalized job dict in-place."""
    title = job.get('title', '')
    desc = job.get('description', '')
    el = infer_entry_level(
        title,
        pay_grade=job.get('pay_grade'),
        salary_max=job.get('salary_max'),
        employment_type=job.get('employment_type'),
        description=desc,
    )
    job.setdefault('sector', infer_sector(title, desc))
    if el is not None:
        job['is_entry_level'] = el
    return job

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


def get_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set in .env")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def parse_location(location_str):
    if not location_str:
        return "", ""
    parts = [p.strip() for p in location_str.split(",")]
    city = parts[0] if parts else ""
    state = ""
    for part in parts[1:]:
        pc = part.strip().upper()
        if pc in STATE_ABBREVS:
            state = pc; break
        if pc in STATE_NAME_TO_ABBREV:
            state = STATE_NAME_TO_ABBREV[pc]; break
    return city, state


def job_fingerprint(title, company, state):
    key = f"{title.lower().strip()}|{company.lower().strip()}|{state.lower().strip()}"
    return hashlib.md5(key.encode()).hexdigest()


def is_public_sector(title, org, description):
    text = f"{title} {org} {description}".lower()
    return any(term in text for term in PUBLIC_SECTOR_TERMS)


def parse_salary(salary_str):
    if not salary_str:
        return None, None, None
    basis = "annual"
    lower = salary_str.lower()
    if "hour" in lower: basis = "hourly"
    elif "month" in lower: basis = "monthly"
    amounts = re.findall(r'[\$]?([\d,]+\.?\d*)', salary_str)
    amounts = [float(a.replace(',', '')) for a in amounts if a]
    if len(amounts) >= 2: return min(amounts), max(amounts), basis
    elif len(amounts) == 1: return amounts[0], amounts[0], basis
    return None, None, None


def infer_org_type(text):
    text = text.lower()
    if any(kw in text for kw in [
        "federal", "u.s. department", "us department", "united states department",
        "usda", "doj", "dod", "irs", "fbi", "gsa", "epa", "fema", "hhs", "nih",
        "cdc", "nasa", "usaid", "cia", "nsa", "dhs", "dot", "hud", "sba", "va ",
        "veterans affairs", "social security", "office of management and budget",
        "office of personnel management", "general services", "u.s. army", "u.s. navy",
        "u.s. air force", "u.s. marine", "u.s. coast guard", "department of defense",
        "department of state", "department of justice", "department of energy",
        "department of health", "department of labor", "department of treasury",
        "department of commerce", "department of interior", "department of education",
        "department of agriculture", "department of homeland", "department of housing",
        "department of transportation", "usajobs", "opm.gov", "general schedule",
        "competitive service", "excepted service", "federal employee",
    ]): return "federal"
    if any(kw in text for kw in [
        "city of", "county of", "town of", "village of", "township", "municipal",
        "metro ", "metropolitan", "transit authority", "port authority",
        "housing authority", "water authority", "public works", "parks and recreation",
        "city council", "mayor", "alderman", "borough", "district of columbia",
        "school district", "unified school", "community college", "public library",
        " mta ", " cta ", " wmata ", " bart ", " septa ",
    ]): return "local"
    if any(kw in text for kw in [
        "state of ", "state department", "state agency", "commonwealth of",
        "state legislature", "state capitol", "state board", "state commission",
        "state police", "state university", "state college", "governor",
        "lieutenant governor", "attorney general", "state treasurer",
        "state comptroller", "department of motor vehicles", "dmv",
    ]): return "state"
    if any(kw in text for kw in [
        "nonprofit", "non-profit", "foundation", "ngo", "association", "charity",
        "501(c)", "501c3", "civil society", "advocacy", "mission-driven",
        "mission driven", "social impact", "philanthropic", "charitable",
        "public interest", "community organization", "think tank",
        "institute for", "center for", "action network", "policy center",
    ]): return "nonprofit"
    return "unknown"


def clean_html(text):
    if not text: return ""
    return re.sub(r'<[^>]+>', '', text).strip()


# ── USAJOBS ──────────────────────────────────────────────────

def fetch_usajobs(keyword="", location="", page=1, results_per_page=250):
    if not USAJOBS_API_KEY or not USAJOBS_EMAIL:
        return {"SearchResult": {"SearchResultItems": [], "SearchResultCount": 0}}
    url = "https://data.usajobs.gov/api/Search"
    headers = {"Authorization-Key": USAJOBS_API_KEY, "User-Agent": USAJOBS_EMAIL, "Host": "data.usajobs.gov"}
    params = {"Page": page, "ResultsPerPage": results_per_page}
    if keyword: params["Keyword"] = keyword
    if location: params["LocationName"] = location
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

def normalize_usajobs(raw_item):
    m = raw_item.get("MatchedObjectDescriptor", {})
    d = m.get("UserArea", {}).get("Details", {})
    rem = (m.get("PositionRemuneration") or [{}])[0]
    loc = (m.get("PositionLocation") or [{}])[0]
    hp = d.get("HiringPath", [])
    if isinstance(hp, str): hp = [hp]
    sl = d.get("PositionSchedule", [])
    emp = "full_time"
    if sl:
        sn = sl[0].get("Name", "").lower() if isinstance(sl[0], dict) else str(sl[0]).lower()
        if "part" in sn: emp = "part_time"
        elif "intern" in sn: emp = "internship"
    tw = d.get("TeleworkEligible", False)
    if isinstance(tw, str): tw = tw.lower() in ("true", "yes", "1")
    title = m.get("PositionTitle", "")
    org = m.get("OrganizationName", "")
    st = loc.get("CountrySubDivisionCode", "")
    return {
        "source": "usajobs", "source_id": m.get("PositionID", ""),
        "title": title, "organization": org, "organization_type": "federal",
        "description": clean_html(m.get("QualificationSummary", "")),
        "qualifications": clean_html(m.get("QualificationSummary", "")),
        "location_city": loc.get("CityName", ""), "location_state": st, "location_country": "US",
        "is_remote": tw,
        "salary_min": float(rem.get("MinimumRange", 0) or 0) or None,
        "salary_max": float(rem.get("MaximumRange", 0) or 0) or None,
        "salary_basis": rem.get("Description", "Per Year"),
        "pay_grade": f"GS-{d.get('LowGrade', '')}" if d.get("LowGrade") else None,
        "employment_type": emp, "schedule": d.get("PositionOfferingType", ""),
        "hiring_path": hp if hp else None,
        "application_url": m.get("ApplyURI", [""])[0] if m.get("ApplyURI") else "",
        "posted_date": m.get("PublicationStartDate"), "closing_date": m.get("ApplicationCloseDate"),
        "fingerprint": job_fingerprint(title, org, st),
        "is_active": True, "raw_data": json.dumps(raw_item),
    }

def fetch_all_usajobs():
    all_n = []; page = 1; tp = 1
    print("  Fetching USAJobs (all current federal listings)...")
    while page <= tp:
        try:
            data = fetch_usajobs(page=page, results_per_page=500)
            sr = data.get("SearchResult", {})
            items = sr.get("SearchResultItems", [])
            tc = int(sr.get("SearchResultCountAll", 0))
            tp = min((tc // 500) + 1, 20)
            for item in items:
                try:
                    n = normalize_usajobs(item)
                    if n["source_id"]: all_n.append(n)
                except Exception as e: print(f"    Normalize error: {e}")
            print(f"    Page {page}/{tp}: got {len(items)} jobs (total so far: {len(all_n)})")
            page += 1; time.sleep(1)
        except Exception as e:
            print(f"    Fetch error on page {page}: {e}"); break
    return all_n


# ── JOOBLE ───────────────────────────────────────────────────

def fetch_jooble(keywords, location, page=1):
    if not JOOBLE_API_KEY: return []
    conn = http.client.HTTPSConnection("jooble.org")
    body = json.dumps({"keywords": keywords, "location": location, "page": str(page)})
    conn.request("POST", "/api/" + JOOBLE_API_KEY, body, {"Content-type": "application/json"})
    resp = conn.getresponse()
    if resp.status != 200: return []
    data = json.loads(resp.read().decode("utf-8"))
    conn.close()
    return data.get("jobs", [])

def normalize_jooble(raw):
    city, state = parse_location(raw.get("location", ""))
    sal_min, sal_max, sal_basis = parse_salary(raw.get("salary", ""))
    title = raw.get("title", ""); company = raw.get("company", "")
    snippet = clean_html(raw.get("snippet", ""))
    sid = str(raw.get("id", ""))
    if not sid: sid = hashlib.md5(raw.get("link", "").encode()).hexdigest()
    return {
        "source": "jooble", "source_id": sid, "title": title, "organization": company,
        "organization_type": infer_org_type(f"{company} {title} {snippet}"),
        "description": snippet, "location_city": city, "location_state": state, "location_country": "US",
        "is_remote": "remote" in title.lower() or "remote" in raw.get("location", "").lower(),
        "salary_min": sal_min, "salary_max": sal_max, "salary_basis": sal_basis,
        "employment_type": raw.get("type", "full_time"),
        "application_url": raw.get("link", ""), "posted_date": raw.get("updated"),
        "fingerprint": job_fingerprint(title, company, state),
        "is_active": True, "raw_data": json.dumps(raw),
    }

def fetch_all_jooble(max_pages_per_query=2):
    all_jobs = {}
    total_q = len(PUBLIC_SECTOR_KEYWORDS) * len(PILOT_STATES); qn = 0
    for kw in PUBLIC_SECTOR_KEYWORDS:
        for loc in PILOT_STATES:
            qn += 1; print(f"    [{qn}/{total_q}] '{kw}' in {loc}")
            for pg in range(1, max_pages_per_query + 1):
                try:
                    jobs = fetch_jooble(kw, loc, page=pg)
                    if not jobs: break
                    for job in jobs:
                        link = job.get("link", "")
                        if link and link not in all_jobs:
                            n = normalize_jooble(job)
                            if n["source_id"] and is_public_sector(n["title"], n["organization"], n.get("description", "")):
                                all_jobs[link] = n
                    time.sleep(0.5)
                except Exception as e: print(f"      Error: {e}"); break
    result = list(all_jobs.values())
    print(f"    Total unique Jooble jobs: {len(result)}")
    return result


# ── ADZUNA ───────────────────────────────────────────────────

def fetch_adzuna(keyword, location, page=1, rpp=50):
    if not ADZUNA_APP_ID or not ADZUNA_APP_KEY: return []
    url = f"https://api.adzuna.com/v1/api/jobs/us/search/{page}"
    params = {"app_id": ADZUNA_APP_ID, "app_key": ADZUNA_APP_KEY, "what": keyword, "where": location, "results_per_page": rpp, "content-type": "application/json"}
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json().get("results", [])

def normalize_adzuna(raw):
    lo = raw.get("location", {}); area = lo.get("area", []); display = lo.get("display_name", "")
    city, state = parse_location(display)
    if not state and len(area) >= 2:
        for p in area[1:]:
            u = p.strip().upper()
            if u in STATE_ABBREVS: state = u; break
            if u in STATE_NAME_TO_ABBREV: state = STATE_NAME_TO_ABBREV[u]; break
    company = raw.get("company", {}).get("display_name", "")
    title = raw.get("title", ""); desc = clean_html(raw.get("description", ""))
    ct = raw.get("contract_time", ""); emp = "full_time"
    if ct == "part_time": emp = "part_time"
    elif "intern" in title.lower(): emp = "internship"
    return {
        "source": "adzuna", "source_id": str(raw.get("id", "")),
        "title": clean_html(title), "organization": company,
        "organization_type": infer_org_type(f"{company} {title} {desc}"),
        "description": desc, "location_city": city, "location_state": state, "location_country": "US",
        "is_remote": "remote" in title.lower() or "remote" in display.lower(),
        "salary_min": raw.get("salary_min"), "salary_max": raw.get("salary_max"), "salary_basis": "annual",
        "job_category": raw.get("category", {}).get("label", ""),
        "employment_type": emp, "schedule": raw.get("contract_type") or None,
        "application_url": raw.get("redirect_url", ""), "posted_date": raw.get("created"),
        "fingerprint": job_fingerprint(title, company, state),
        "is_active": True, "raw_data": json.dumps(raw),
    }

def fetch_all_adzuna(max_pages_per_query=2):
    all_jobs = {}
    total_q = len(PUBLIC_SECTOR_KEYWORDS) * len(PILOT_STATES); qn = 0
    for kw in PUBLIC_SECTOR_KEYWORDS:
        for loc in PILOT_STATES:
            qn += 1; print(f"    [{qn}/{total_q}] '{kw}' in {loc}")
            for pg in range(1, max_pages_per_query + 1):
                try:
                    jobs = fetch_adzuna(kw, loc, page=pg)
                    if not jobs: break
                    for job in jobs:
                        jid = str(job.get("id", ""))
                        if jid and jid not in all_jobs:
                            n = normalize_adzuna(job)
                            if n["source_id"]: all_jobs[jid] = n
                    time.sleep(0.3)
                except Exception as e: print(f"      Error: {e}"); break
    result = list(all_jobs.values())
    print(f"    Total unique Adzuna jobs: {len(result)}")
    return result


# ── SERPAPI (GOOGLE JOBS) ────────────────────────────────────

def fetch_serpapi(query, location="United States"):
    if not SERPAPI_KEY: print("  WARNING: SERPAPI_KEY not set"); return []
    params = {"engine": "google_jobs", "q": query, "location": location, "api_key": SERPAPI_KEY}
    response = requests.get("https://serpapi.com/search.json", params=params)
    response.raise_for_status()
    return response.json().get("jobs_results", [])

def normalize_serpapi(raw):
    title = raw.get("title", ""); company = raw.get("company_name", "")
    location = raw.get("location", ""); city, state = parse_location(location)
    desc = raw.get("description", ""); ext = raw.get("detected_extensions", {})
    sched = ext.get("schedule_type", ""); emp = "full_time"
    if "part" in sched.lower(): emp = "part_time"
    elif "intern" in title.lower(): emp = "internship"
    return {
        "source": "google_jobs", "source_id": hashlib.md5(f"{title}{company}{location}".encode()).hexdigest(),
        "title": title, "organization": company, "organization_type": infer_org_type(f"{company} {title} {desc}"),
        "description": desc[:2000], "location_city": city, "location_state": state, "location_country": "US",
        "is_remote": ext.get("work_from_home", False),
        "salary_min": ext.get("salary_min"), "salary_max": ext.get("salary_max"),
        "salary_basis": "annual" if ext.get("salary_min") else None,
        "employment_type": emp, "fingerprint": job_fingerprint(title, company, state),
        "is_active": True, "raw_data": json.dumps(raw),
    }

def fetch_all_serpapi(max_queries=14):
    all_jobs = {}
    queries = [
        # Entry-level focused
        "entry level government jobs",
        "junior analyst government entry level",
        "recent graduate federal government jobs",
        "government fellowship program entry level",
        "public policy associate entry level",
        # Sector-specific entry level
        "entry level city government jobs",
        "government technology jobs entry level",
        "public health coordinator entry level",
        "social services case manager entry level",
        "political campaign field organizer",
        # Broader pipeline
        "state government jobs recent graduate",
        "nonprofit policy jobs entry level",
        "government internship paid",
        "public administration entry level",
    ]
    for i, q in enumerate(queries[:max_queries]):
        print(f"    [{i+1}/{min(len(queries), max_queries)}] '{q}'")
        try:
            for job in fetch_serpapi(q):
                key = hashlib.md5(f"{job.get('title','')}{job.get('company_name','')}".encode()).hexdigest()
                if key not in all_jobs: all_jobs[key] = normalize_serpapi(job)
            time.sleep(1)
        except Exception as e: print(f"      Error: {e}")
    result = list(all_jobs.values())
    print(f"    Total unique Google Jobs: {len(result)}")
    return result


# ── FINDWORK ─────────────────────────────────────────────────

def fetch_findwork(search="", location="", page=1):
    if not FINDWORK_API_KEY: print("  WARNING: FINDWORK_API_KEY not set"); return []
    params = {"search": search, "location": location, "page": page, "sort_by": "date"}
    headers = {"Authorization": f"Token {FINDWORK_API_KEY}"}
    resp = requests.get("https://findwork.dev/api/jobs/", params=params, headers=headers)
    resp.raise_for_status()
    return resp.json().get("results", [])

def normalize_findwork(raw):
    title = raw.get("role", ""); company = raw.get("company_name", "")
    location = raw.get("location", ""); city, state = parse_location(location)
    desc = clean_html(raw.get("text", ""))
    return {
        "source": "findwork", "source_id": f"findwork_{raw.get('id', '')}",
        "title": title, "organization": company,
        "organization_type": infer_org_type(f"{company} {title} {desc}"),
        "description": desc[:2000], "location_city": city, "location_state": state,
        "location_country": raw.get("country_iso", "US"),
        "is_remote": raw.get("remote", False),
        "employment_type": raw.get("employment_type", "full_time").lower().replace("-", "_").replace(" ", "_"),
        "application_url": raw.get("url", ""), "posted_date": raw.get("date_posted"),
        "fingerprint": job_fingerprint(title, company, state),
        "is_active": True, "raw_data": json.dumps(raw),
    }

def fetch_all_findwork():
    all_jobs = {}
    queries = ["government", "public policy", "public health", "nonprofit", "federal"]
    for i, q in enumerate(queries):
        print(f"    [{i+1}/{len(queries)}] '{q}'")
        try:
            for pg in range(1, 4):
                jobs = fetch_findwork(search=q, page=pg)
                if not jobs: break
                for job in jobs:
                    jid = str(job.get("id", ""))
                    if jid and jid not in all_jobs:
                        n = normalize_findwork(job)
                        if n.get("location_country", "").upper() in ("US", "UNITED STATES", ""):
                            all_jobs[jid] = n
                time.sleep(0.5)
        except Exception as e: print(f"      Error: {e}")
    result = list(all_jobs.values())
    print(f"    Total unique Findwork jobs: {len(result)}")
    return result


# ── JOBICY (RSS) ─────────────────────────────────────────────

def fetch_all_jobicy():
    try:
        resp = requests.get("https://jobicy.com/feed/newjobs"); resp.raise_for_status()
        root = ET.fromstring(resp.content)
        normalized = []
        for item in root.findall(".//item"):
            title = item.findtext("title", "")
            desc = clean_html(item.findtext("description", ""))
            cats = [c.text for c in item.findall("category") if c.text]
            link = item.findtext("link", "")
            if is_public_sector(title, "", desc):
                normalized.append({
                    "source": "jobicy",
                    "source_id": f"jobicy_{hashlib.md5(link.encode()).hexdigest()}",
                    "title": title, "organization": "", "organization_type": infer_org_type(f"{title} {desc}"),
                    "description": desc[:2000], "location_city": "", "location_state": "",
                    "location_country": "US", "is_remote": True, "employment_type": "full_time",
                    "application_url": link, "posted_date": item.findtext("pubDate"),
                    "fingerprint": job_fingerprint(title, "", ""),
                    "is_active": True, "raw_data": json.dumps({"title": title, "link": link, "categories": cats}),
                })
        print(f"    Total Jobicy jobs (filtered): {len(normalized)}")
        return normalized
    except Exception as e:
        print(f"    Error fetching Jobicy: {e}"); return []


# ── IDEALIST (nonprofit / civic sector RSS) ──────────────────

def fetch_all_idealist():
    """
    Idealist.org has an unofficial RSS feed for nonprofit/civic jobs.
    Pulls entry-level and professional roles from advocacy, policy, and
    social-sector orgs — a major pipeline for public service careers.
    """
    IDEALIST_FEEDS = [
        "https://www.idealist.org/en/api/jobs/search.rss?type=JOB&q=policy+analyst",
        "https://www.idealist.org/en/api/jobs/search.rss?type=JOB&q=community+outreach",
        "https://www.idealist.org/en/api/jobs/search.rss?type=JOB&q=social+services",
        "https://www.idealist.org/en/api/jobs/search.rss?type=JOB&q=public+health",
        "https://www.idealist.org/en/api/jobs/search.rss?type=INTERNSHIP&q=government",
        "https://www.idealist.org/en/api/jobs/search.rss?type=JOB&q=government+affairs",
        "https://www.idealist.org/en/api/jobs/search.rss?type=JOB&q=advocacy",
    ]
    normalized = []; seen_links = set()
    for feed_url in IDEALIST_FEEDS:
        try:
            resp = requests.get(feed_url, timeout=15)
            if resp.status_code != 200:
                continue
            root = ET.fromstring(resp.content)
            for item in root.findall(".//item"):
                title = item.findtext("title", "")
                link  = item.findtext("link", "")
                desc  = clean_html(item.findtext("description", ""))
                org   = ""
                # Some Idealist feeds embed org in title "Role | Org Name"
                if " | " in title:
                    parts = title.split(" | ")
                    title = parts[0].strip()
                    org   = parts[-1].strip()
                if not link or link in seen_links:
                    continue
                if not is_public_sector(title, org, desc):
                    continue
                seen_links.add(link)
                city, state = "", ""
                loc_el = item.find("{http://www.idealist.org/ns/}location")
                if loc_el is not None and loc_el.text:
                    city, state = parse_location(loc_el.text)
                normalized.append({
                    "source": "idealist",
                    "source_id": f"idealist_{hashlib.md5(link.encode()).hexdigest()}",
                    "title": title, "organization": org,
                    "organization_type": infer_org_type(f"{org} {title} {desc}"),
                    "description": desc[:2000],
                    "location_city": city, "location_state": state, "location_country": "US",
                    "is_remote": "remote" in desc.lower() or "remote" in title.lower(),
                    "employment_type": "internship" if "intern" in title.lower() else "full_time",
                    "application_url": link,
                    "posted_date": item.findtext("pubDate"),
                    "fingerprint": job_fingerprint(title, org, state),
                    "is_active": True,
                    "raw_data": json.dumps({"title": title, "link": link, "org": org}),
                })
            time.sleep(0.5)
        except Exception as e:
            print(f"    Idealist feed error ({feed_url}): {e}")
    print(f"    Total Idealist jobs (filtered): {len(normalized)}")
    return normalized


# ── CAREERJET (disabled from auto-sync) ──────────────────────

def fetch_all_careerjet(max_pages=2):
    all_jobs = {}
    for kw in PUBLIC_SECTOR_KEYWORDS:
        for loc in PILOT_STATES:
            for pg in range(1, max_pages + 1):
                try:
                    params = {"keywords": kw, "location": loc, "affid": "publicpath", "user_ip": "0.0.0.0",
                        "user_agent": "Mozilla/5.0", "url": "https://tahvia127.github.io/PublicPath/jobs.html",
                        "sort": "date", "pagesize": 50, "page": pg, "locale_code": "en_US"}
                    resp = requests.get("http://public.api.careerjet.net/search", params=params)
                    resp.raise_for_status()
                    jobs = resp.json().get("jobs", [])
                    if not jobs: break
                    for job in jobs:
                        u = job.get("url", "")
                        if u and u not in all_jobs:
                            title = job.get("title", ""); company = job.get("company", "")
                            city, state = parse_location(job.get("locations", ""))
                            all_jobs[u] = {
                                "source": "careerjet", "source_id": hashlib.md5(u.encode()).hexdigest(),
                                "title": title, "organization": company,
                                "organization_type": infer_org_type(f"{company} {title}"),
                                "description": clean_html(job.get("description", "")),
                                "location_city": city, "location_state": state, "location_country": "US",
                                "is_remote": "remote" in title.lower(), "employment_type": "full_time",
                                "application_url": u, "posted_date": job.get("date"),
                                "fingerprint": job_fingerprint(title, company, state),
                                "is_active": True, "raw_data": json.dumps(job),
                            }
                    time.sleep(0.5)
                except Exception as e: print(f"      Error: {e}"); break
    result = list(all_jobs.values())
    print(f"    Total unique Careerjet jobs: {len(result)}")
    return result


# ── SYNC ENGINE ──────────────────────────────────────────────

def upsert_jobs(supabase, jobs, source_name, batch_size=50):
    stats = {"inserted": 0, "errors": 0, "error_messages": []}
    seen = set(); cleaned = []
    blocked = 0
    for job in jobs:
        key = (job.get("source", ""), job.get("source_id", ""))
        if key in seen or not key[1]: continue
        # Skip blocklisted organizations
        org = job.get("organization", "").lower().strip()
        if org in ORG_BLOCKLIST:
            blocked += 1
            continue
        seen.add(key)
        job = enrich_job(job)
        cleaned.append({k: v for k, v in job.items() if v is not None})
    if blocked:
        print(f"  Blocked {blocked} jobs from blocklisted orgs")
    print(f"  Deduplicated: {len(jobs)} -> {len(cleaned)} unique jobs")
    for i in range(0, len(cleaned), batch_size):
        batch = cleaned[i:i + batch_size]
        try:
            supabase.table("jobs").upsert(batch, on_conflict="source,source_id").execute()
            stats["inserted"] += len(batch)
        except Exception as e:
            stats["errors"] += len(batch)
            stats["error_messages"].append(str(e))
            print(f"    Upsert error (batch {i//batch_size + 1}): {e}")
    return stats

def sync_source(supabase, source_name, fetch_fn, **kwargs):
    print(f"\n{'='*60}\nSYNCING: {source_name.upper()}\n{'='*60}")
    log_entry = {"source": source_name, "started_at": datetime.utcnow().isoformat(),
        "jobs_fetched": 0, "jobs_new": 0, "jobs_updated": 0, "errors": []}
    try:
        normalized_jobs = fetch_fn(**kwargs)
        log_entry["jobs_fetched"] = len(normalized_jobs)
        print(f"  Fetched {len(normalized_jobs)} jobs")
        if normalized_jobs:
            stats = upsert_jobs(supabase, normalized_jobs, source_name)
            log_entry["jobs_new"] = stats["inserted"]
            log_entry["errors"] = stats["error_messages"]
            print(f"  Upserted: {stats['inserted']}, Errors: {stats['errors']}")
        else:
            print("  No jobs to upsert")
    except Exception as e:
        log_entry["errors"] = [str(e)]; print(f"  FATAL ERROR: {e}")
    log_entry["completed_at"] = datetime.utcnow().isoformat()
    try: supabase.table("sync_log").insert(log_entry).execute()
    except Exception as e: print(f"  Could not write sync log: {e}")
    return log_entry

def dedup_cross_source(supabase):
    try:
        result = supabase.rpc("dedup_cross_source_jobs").execute()
        count = result.data if result.data else 0
        print(f"  Deactivated {count} cross-source duplicates")
    except Exception as e: print(f"  Error in cross-source dedup: {e}")

def expire_old_jobs(supabase):
    print(f"\n{'='*60}\nEXPIRING OLD JOBS\n{'='*60}")
    try:
        result = supabase.rpc("deactivate_expired_jobs").execute()
        print(f"  Deactivated {result.data if result.data else 0} expired jobs")
    except Exception as e: print(f"  Error: {e}")

def print_stats(supabase):
    print(f"\n{'='*60}\nDATABASE STATS\n{'='*60}")
    try:
        stats = supabase.rpc("get_job_stats").execute().data
        if stats:
            print(f"  Total jobs:           {stats.get('total_jobs', 0)}")
            print(f"  Active jobs:          {stats.get('active_jobs', 0)}")
            print(f"  Closing in 7 days:    {stats.get('closing_within_7_days', 0)}")
            print(f"\n  By Source:")
            for s, c in (stats.get("by_source") or {}).items(): print(f"    {s}: {c}")
            print(f"\n  By Org Type:")
            for o, c in (stats.get("by_org_type") or {}).items(): print(f"    {o}: {c}")
            print(f"\n  By State (top 15):")
            for s, c in sorted((stats.get("by_state") or {}).items(), key=lambda x: -x[1])[:15]: print(f"    {s}: {c}")
            last = stats.get("last_sync")
            if last: print(f"\n  Last sync: {last.get('source')} at {last.get('completed_at')} ({last.get('jobs_fetched')} jobs)")
    except Exception as e: print(f"  Error fetching stats: {e}")


# ── MAIN ─────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="PublicPath Job Sync Pipeline")
    parser.add_argument("--source",
        choices=["usajobs", "jooble", "adzuna", "serpapi", "findwork", "jobicy", "idealist", "careerjet", "all"],
        default="all", help="Which source to sync (default: all)")
    parser.add_argument("--stats", action="store_true", help="Print database stats and exit")
    parser.add_argument("--expire", action="store_true", help="Deactivate expired jobs and exit")
    parser.add_argument("--jooble-pages", type=int, default=2, help="Max pages per Jooble query")
    args = parser.parse_args()

    supabase = get_supabase()

    if args.stats: print_stats(supabase); return
    if args.expire: expire_old_jobs(supabase); return

    print(f"{'='*60}\nPublicPath Job Sync — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}\n{'='*60}")

    if args.source in ("usajobs", "all"):   sync_source(supabase, "usajobs",    fetch_all_usajobs)
    if args.source in ("jooble", "all"):    sync_source(supabase, "jooble",     fetch_all_jooble, max_pages_per_query=args.jooble_pages)
    if args.source in ("adzuna", "all"):    sync_source(supabase, "adzuna",     fetch_all_adzuna)
    if args.source in ("serpapi", "all"):   sync_source(supabase, "google_jobs",fetch_all_serpapi)
    if args.source in ("findwork", "all"):  sync_source(supabase, "findwork",   fetch_all_findwork)
    if args.source in ("jobicy", "all"):    sync_source(supabase, "jobicy",     fetch_all_jobicy)
    if args.source in ("idealist", "all"):  sync_source(supabase, "idealist",   fetch_all_idealist)
    if args.source == "careerjet":          sync_source(supabase, "careerjet",  fetch_all_careerjet)

    expire_old_jobs(supabase)

    print(f"\n{'='*60}\nPOST-SYNC CLEANUP\n{'='*60}")
    for fn, label in [
        ("classify_experience_levels", "Classified experience levels"),
        ("normalize_employment_types", "Normalized employment types"),
        ("normalize_state_names", "Normalized state names"),
        ("reclassify_org_types", "Reclassified org types"),
        ("classify_job_sectors",  "Classified job sectors"),
        ("classify_entry_levels", "Classified entry-level flags"),
    ]:
        try: supabase.rpc(fn).execute(); print(f"  {label}")
        except Exception as e: print(f"  Error ({fn}): {e}")

    dedup_cross_source(supabase)
    print_stats(supabase)
    print(f"\n{'='*60}\nSYNC COMPLETE\n{'='*60}")

if __name__ == "__main__":
    main()
