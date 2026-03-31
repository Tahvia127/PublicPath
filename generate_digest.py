"""
PublicPath — Weekly Digest Generator
=====================================
Generates personalized weekly digest content from the jobs database.
Pulls jobs matched to subscriber preferences and includes Hira's network picks.

Usage:
    python generate_digest.py                  # Generate full digest
    python generate_digest.py --segment federal # Generate for one segment
    python generate_digest.py --preview        # Preview without formatting
    python generate_digest.py --stats          # Show subscriber preference stats

Output:
    Prints formatted digest content for each subscriber segment.
    Copy/paste into Beehiiv to send.

Setup:
    Same .env file as sync_jobs.py
"""

import argparse
import os
import sys
from datetime import datetime, timedelta

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")


def get_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set in .env")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def get_subscriber_stats(supabase):
    """Show what preferences subscribers have selected."""
    result = supabase.table("subscribers").select("*").eq("is_active", True).execute()
    subs = result.data or []

    print(f"\n{'='*60}")
    print(f"SUBSCRIBER STATS — {len(subs)} active subscribers")
    print(f"{'='*60}")

    if not subs:
        print("  No subscribers yet.")
        return

    # Count government level preferences
    gov_counts = {}
    func_counts = {}
    school_counts = {}

    for s in subs:
        for g in (s.get("gov_levels") or []):
            gov_counts[g] = gov_counts.get(g, 0) + 1
        for f in (s.get("func_areas") or []):
            func_counts[f] = func_counts.get(f, 0) + 1
        school = s.get("school", "Unknown")
        if school:
            school_counts[school] = school_counts.get(school, 0) + 1

    print("\n  Government Level Preferences:")
    for k, v in sorted(gov_counts.items(), key=lambda x: -x[1]):
        print(f"    {k}: {v} subscribers")

    print("\n  Function Area Preferences:")
    for k, v in sorted(func_counts.items(), key=lambda x: -x[1]):
        print(f"    {k}: {v} subscribers")

    print("\n  Schools:")
    for k, v in sorted(school_counts.items(), key=lambda x: -x[1]):
        print(f"    {k}: {v}")


def get_network_picks(supabase):
    """Get Hira's manually added network jobs."""
    result = supabase.table("network_picks") \
        .select("*") \
        .eq("include_in_digest", True) \
        .order("created_at", desc=True) \
        .limit(5) \
        .execute()
    return result.data or []


def get_jobs_for_segment(supabase, gov_level=None, func_area=None, state=None, limit=10):
    """Get entry-level jobs matching a preference segment."""
    now = datetime.utcnow().isoformat()
    future = (datetime.utcnow() + timedelta(days=21)).isoformat()

    query = supabase.table("jobs") \
        .select("title, organization, organization_type, location_city, location_state, salary_min, salary_max, closing_date, application_url") \
        .eq("is_active", True) \
        .eq("experience_level", "entry") \
        .gt("closing_date", now) \
        .lt("closing_date", future)

    if gov_level:
        query = query.eq("organization_type", gov_level)

    if state:
        query = query.eq("location_state", state)

    # For func_area, search title and description
    if func_area:
        area_keywords = {
            "policy": "policy",
            "operations": "operations",
            "technology": "technology",
            "comms": "communications",
            "finance": "budget",
            "legal": "attorney",
        }
        kw = area_keywords.get(func_area, func_area)
        query = query.ilike("title", f"%{kw}%")

    result = query.order("closing_date").limit(limit).execute()
    return result.data or []


def format_job(job, include_salary=True):
    """Format a single job for the digest."""
    title = job.get("title", "Untitled")
    org = job.get("organization", "")
    city = job.get("location_city", "")
    state = job.get("location_state", "")
    location = ", ".join(filter(None, [city, state]))

    salary = ""
    if include_salary and job.get("salary_min") and job.get("salary_max"):
        sal_min = f"${int(job['salary_min']):,}"
        sal_max = f"${int(job['salary_max']):,}"
        salary = f" | {sal_min} - {sal_max}"

    closing = ""
    if job.get("closing_date"):
        try:
            dt = datetime.fromisoformat(job["closing_date"].replace("Z", "+00:00"))
            closing = f" | Closes {dt.strftime('%b %d')}"
        except:
            pass

    url = job.get("application_url", "")
    link = job.get("url", url)

    return {
        "title": title,
        "org": org,
        "location": location,
        "salary": salary,
        "closing": closing,
        "url": link,
    }


def format_network_pick(pick):
    """Format a network pick for the digest."""
    return {
        "title": pick.get("title", ""),
        "org": pick.get("organization", ""),
        "location": pick.get("location", ""),
        "notes": pick.get("notes", ""),
        "source_contact": pick.get("source_contact", ""),
        "url": pick.get("url", ""),
    }


def print_digest_text(segment_name, jobs, network_picks, fellowships):
    """Print the full digest in copyable text format."""
    print(f"\n{'='*60}")
    print(f"DIGEST: {segment_name.upper()}")
    print(f"{'='*60}")

    today = datetime.utcnow().strftime("%B %d, %Y")
    print(f"\nPublicPath Weekly — {today}")
    print(f"Each week we hand-pick roles from across federal, state, and")
    print(f"local government — plus opportunities shared directly by")
    print(f"people working in the field.\n")

    # Section 1: Top Picks
    print("─" * 50)
    print("THIS WEEK'S TOP PICKS")
    print("─" * 50)

    if jobs:
        # Group by org type
        federal = [j for j in jobs if j.get("organization_type") == "federal"]
        state_local = [j for j in jobs if j.get("organization_type") in ("state", "local")]
        other = [j for j in jobs if j.get("organization_type") not in ("federal", "state", "local")]

        for group_name, group in [("Federal", federal), ("State & Local", state_local), ("Other", other)]:
            if not group:
                continue
            print(f"\n  {group_name}:")
            for job in group:
                f = format_job(job)
                print(f"  • {f['title']}")
                print(f"    {f['org']} — {f['location']}{f['salary']}{f['closing']}")
                if f['url']:
                    print(f"    Apply: {f['url']}")
                print()
    else:
        print("  No matching jobs found for this segment.\n")

    # Section 2: From Our Network
    print("─" * 50)
    print("FROM OUR NETWORK")
    print("─" * 50)

    if network_picks:
        for pick in network_picks:
            f = format_network_pick(pick)
            print(f"\n  • {f['title']}")
            org_line = f"    {f['org']}"
            if f['location']:
                org_line += f" — {f['location']}"
            print(org_line)
            if f['source_contact']:
                print(f"    Shared by {f['source_contact']}")
            if f['notes']:
                print(f"    {f['notes']}")
            if f['url']:
                print(f"    Apply: {f['url']}")
            print()
    else:
        print("  No network picks this week. Hira can add them in Supabase > network_picks table.\n")

    # Section 3: On Our Radar
    print("─" * 50)
    print("ON OUR RADAR")
    print("─" * 50)

    if fellowships:
        for job in fellowships:
            f = format_job(job, include_salary=False)
            print(f"\n  • {f['title']}")
            print(f"    {f['org']} — {f['location']}{f['closing']}")
            if f['url']:
                print(f"    More info: {f['url']}")
            print()
    else:
        print("  No fellowships or programs closing soon.\n")

    print("─" * 50)
    print(f"Browse all {segment_name} jobs: https://tahvia127.github.io/PublicPath/jobs.html")
    print("─" * 50)


def print_digest_html(segment_name, jobs, network_picks, fellowships):
    """Print digest as HTML you can paste directly into Beehiiv."""
    today = datetime.utcnow().strftime("%B %d, %Y")

    print(f"\n<!-- ===== BEEHIIV HTML: {segment_name.upper()} ===== -->")
    print(f"<p><em>PublicPath Weekly — {today}</em><br>")
    print(f"Each week we hand-pick roles from across federal, state, and local government — plus opportunities shared directly by people working in the field.</p>")

    # Section 1
    print(f"\n<h2>This Week's Top Picks</h2>")

    if jobs:
        federal = [j for j in jobs if j.get("organization_type") == "federal"]
        state_local = [j for j in jobs if j.get("organization_type") in ("state", "local")]
        other = [j for j in jobs if j.get("organization_type") not in ("federal", "state", "local")]

        for group_name, group in [("Federal", federal), ("State & Local", state_local), ("Other", other)]:
            if not group:
                continue
            print(f"\n<p><strong>{group_name}</strong></p>")
            for job in group:
                f = format_job(job)
                closing_html = f"<span style='color:#0891B2'>{f['closing'].replace(' | ', '')}</span>" if f['closing'] else ""
                print(f"<p><strong><a href='{f['url']}'>{f['title']}</a></strong><br>")
                print(f"{f['org']} — {f['location']}{f['salary']}<br>")
                if closing_html:
                    print(f"{closing_html}</p>")
                else:
                    print(f"</p>")
    else:
        print("<p>No matching jobs found this week.</p>")

    # Section 2
    print(f"\n<h2>From Our Network</h2>")

    if network_picks:
        for pick in network_picks:
            f = format_network_pick(pick)
            source = f" · <em>Shared by {f['source_contact']}</em>" if f['source_contact'] else ""
            print(f"<p><strong><a href='{f['url']}'>{f['title']}</a></strong><br>")
            print(f"{f['org']}")
            if f['location']:
                print(f" — {f['location']}")
            print(f"{source}")
            if f['notes']:
                print(f"<br>{f['notes']}")
            print(f"</p>")
    else:
        print("<p>No network picks this week.</p>")

    # Section 3
    print(f"\n<h2>On Our Radar</h2>")

    if fellowships:
        for job in fellowships:
            f = format_job(job, include_salary=False)
            print(f"<p><strong><a href='{f['url']}'>{f['title']}</a></strong><br>")
            print(f"{f['org']} — {f['location']}{f['closing']}</p>")
    else:
        print("<p>No fellowships or programs to flag this week.</p>")

    print(f"\n<p><a href='https://tahvia127.github.io/PublicPath/jobs.html'>Browse all jobs on PublicPath →</a></p>")
    print(f"\n<!-- ===== END {segment_name.upper()} ===== -->")


def generate_digest(supabase, segment=None, html=False):
    """Generate the full weekly digest for all segments or one specific segment."""

    # Get network picks (same for all segments)
    network_picks = get_network_picks(supabase)

    # Get fellowships (same for all segments)
    fellowships = get_jobs_for_segment(supabase, limit=3)
    # Filter to just fellowships
    fellowship_result = supabase.table("jobs") \
        .select("title, organization, organization_type, location_city, location_state, closing_date, application_url") \
        .eq("is_active", True) \
        .or_("title.ilike.%fellow%,title.ilike.%intern%,title.ilike.%program%") \
        .gt("closing_date", datetime.utcnow().isoformat()) \
        .lt("closing_date", (datetime.utcnow() + timedelta(days=30)).isoformat()) \
        .order("closing_date") \
        .limit(3) \
        .execute()
    fellowships = fellowship_result.data or []

    # Define segments based on subscriber preferences
    segments = {
        "federal": {"gov_level": "federal", "label": "Federal Focus"},
        "state_local": {"gov_level": None, "label": "State & Local Focus"},
        "all": {"gov_level": None, "label": "All Government Levels"},
    }

    if segment and segment in segments:
        seg = segments[segment]
        jobs = get_jobs_for_segment(supabase, gov_level=seg["gov_level"], limit=10)
        if segment == "state_local":
            jobs_state = get_jobs_for_segment(supabase, gov_level="state", limit=5)
            jobs_local = get_jobs_for_segment(supabase, gov_level="local", limit=5)
            jobs = jobs_state + jobs_local
        output_fn = print_digest_html if html else print_digest_text
        output_fn(seg["label"], jobs, network_picks, fellowships)
    else:
        # Generate all segments
        for seg_key, seg in segments.items():
            if seg_key == "state_local":
                jobs_state = get_jobs_for_segment(supabase, gov_level="state", limit=5)
                jobs_local = get_jobs_for_segment(supabase, gov_level="local", limit=5)
                jobs = jobs_state + jobs_local
            elif seg_key == "federal":
                jobs = get_jobs_for_segment(supabase, gov_level="federal", limit=10)
            else:
                jobs = get_jobs_for_segment(supabase, limit=10)
            output_fn = print_digest_html if html else print_digest_text
            output_fn(seg["label"], jobs, network_picks, fellowships)


def main():
    parser = argparse.ArgumentParser(description="PublicPath Weekly Digest Generator")
    parser.add_argument("--segment", choices=["federal", "state_local", "all"],
                        help="Generate for one segment only")
    parser.add_argument("--html", action="store_true",
                        help="Output as HTML (for pasting into Beehiiv)")
    parser.add_argument("--stats", action="store_true",
                        help="Show subscriber preference stats")
    args = parser.parse_args()

    supabase = get_supabase()

    if args.stats:
        get_subscriber_stats(supabase)
        return

    generate_digest(supabase, segment=args.segment, html=args.html)


if __name__ == "__main__":
    main()
