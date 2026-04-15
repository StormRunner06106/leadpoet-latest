"""
Pipeline: validate leads via simulate_submit, fix failures via lead_updater, repeat.

Loop per lead:
  1. Run all simulate_submit checks
  2. If failures, apply targeted fixers from lead_updater
  3. Re-validate
  4. Repeat until clean or max retries (for OpenAI-based fixes)
  5. Leads that can't be fixed are removed

At the end, save the cleaned leads back to the target file.

Usage:
    python data/lead_pipeline.py                          # process leads.json
    python data/lead_pipeline.py --file leads-1.json      # specific file
    python data/lead_pipeline.py --max-retries 5          # more OpenAI retries
    python data/lead_pipeline.py --dry-run                # preview, don't save

Requires: OPENAI_API_KEY environment variable.
"""

import argparse
import json
import os
import re
import sys
import time

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DATA_DIR)
sys.path.insert(0, PROJECT_ROOT)

# ── Import validators from simulate_submit ───────────────────
from simulate_submit import (
    validate_lead,
    check_required_fields,
    check_name,
    check_role,
    check_description,
    check_industry,
    check_contact_location,
    check_hq_location,
    check_email_domain,
    check_employee_count,
    check_source,
    check_linkedin,
)

# ── Import fixers from lead_updater ──────────────────────────
from lead_updater import (
    ALLOWED_COUNTRIES,
    _clean_name_part,
    _fix_role_openai,
    _fix_city_openai,
    _DESC_NAV_PATTERN,
    client,
)

# ═══════════════════════════════════════════════════════════════
#  Targeted fixers - each handles specific failure codes
# ═══════════════════════════════════════════════════════════════

def fix_name(lead: dict) -> bool:
    """Fix name fields. Returns True if changed."""
    first_orig = lead.get("first", "").strip()
    last_orig = lead.get("last", "").strip()

    first = _clean_name_part(first_orig)
    last = _clean_name_part(last_orig)

    if first and first == first.lower():
        first = first[0].upper() + first[1:]
    if last and last == last.lower():
        last = last[0].upper() + last[1:]

    if first.lower() == last.lower() and first:
        return False

    full_name = f"{first} {last}".strip()
    changed = (first != first_orig or last != last_orig
               or full_name != lead.get("full_name", ""))
    if changed:
        lead["first"] = first
        lead["last"] = last
        lead["full_name"] = full_name
    return changed


def fix_role(lead: dict) -> bool:
    """Fix role via OpenAI. Returns True if changed."""
    role = lead.get("role", "").strip()
    if not role:
        return False
    new_role = _fix_role_openai(role)
    if new_role and new_role != role:
        lead["role"] = new_role
        return True
    return False


def fix_description(lead: dict) -> bool:
    """Clean navigation links from description. Returns True if changed."""
    desc = lead.get("description", "")
    cleaned = _DESC_NAV_PATTERN.sub('', desc).strip()
    if cleaned != desc:
        lead["description"] = cleaned
        return True
    return False


def fix_contact_location(lead: dict) -> bool:
    """Fix city/hq_city via OpenAI. Returns True if changed."""
    changed = False

    city = lead.get("city", "").strip()
    state = lead.get("state", "").strip()
    if city:
        new_city = _fix_city_openai(city, state)
        if new_city and new_city != city:
            lead["city"] = new_city
            changed = True

    return changed


def fix_hq_location(lead: dict) -> bool:
    """Fix hq_city via OpenAI. Returns True if changed."""
    changed = False

    hq_city = lead.get("hq_city", "").strip()
    hq_state = lead.get("hq_state", "").strip()
    if hq_city:
        new_hq = _fix_city_openai(hq_city, hq_state)
        if new_hq and new_hq != hq_city:
            lead["hq_city"] = new_hq
            changed = True

    return changed


# Map check names to their fixer functions.
# Checks without a fixer = lead gets removed if it fails.
FIXERS = {
    "Name Sanity": fix_name,
    "Role Sanity": fix_role,
    "Description Sanity": fix_description,
    "Contact Location": fix_contact_location,
    "HQ Location": fix_hq_location,
}

# Failures that mean the lead should be removed outright (not fixable).
REMOVE_CODES = {
    "missing_required_fields",
    "invalid_region",          # non-US/UAE contact
    "free_email_domain",
    "email_domain_mismatch",
    "missing_website",
    "invalid_company_linkedin",
    "name_duplicate",          # first == last, can't auto-fix
}


# ═══════════════════════════════════════════════════════════════
#  Pipeline
# ═══════════════════════════════════════════════════════════════

def process_lead(lead: dict, max_retries: int, verbose: bool) -> bool:
    """
    Validate and fix a single lead in a loop.
    Returns True if the lead passes all checks, False if it should be removed.
    """
    lead_id = lead.get("id", "?")
    business = lead.get("business", "?")

    for attempt in range(1, max_retries + 1):
        failures = validate_lead(lead, verbose=False)

        if not failures:
            if attempt > 1 and verbose:
                print(f"  #{lead_id} {business}: PASSED (fixed in {attempt - 1} attempt(s))")
            return True

        # Check if any failure is unfixable -> remove
        for check_name, code, msg in failures:
            if code in REMOVE_CODES:
                if verbose:
                    print(f"  #{lead_id} {business}: REMOVED [{code}] {msg}")
                return False

        # Try to fix each failure
        fixed_any = False
        for check_name, code, msg in failures:
            fixer = FIXERS.get(check_name)
            if fixer:
                try:
                    if fixer(lead):
                        fixed_any = True
                        if verbose:
                            print(f"  #{lead_id} {business}: fixed [{check_name}] (attempt {attempt})")
                except Exception as e:
                    if verbose:
                        print(f"  #{lead_id} {business}: fixer error [{check_name}]: {e}")

        if not fixed_any:
            # No fixer could help and no REMOVE_CODE -> unfixable, remove
            if verbose:
                codes = [c for _, c, _ in failures]
                print(f"  #{lead_id} {business}: REMOVED (unfixable: {codes})")
            return False

        time.sleep(0.05)

    # Exhausted retries - check one final time
    failures = validate_lead(lead, verbose=False)
    if not failures:
        if verbose:
            print(f"  #{lead_id} {business}: PASSED (fixed after {max_retries} retries)")
        return True

    if verbose:
        codes = [c for _, c, _ in failures]
        print(f"  #{lead_id} {business}: REMOVED (still failing after {max_retries} retries: {codes})")
    return False


def run_pipeline(file_path: str, max_retries: int = 3, dry_run: bool = False, verbose: bool = True):
    """Run the full validate-fix loop on a leads JSON file."""
    print(f"\n{'=' * 60}")
    print(f"Pipeline: {os.path.basename(file_path)}")
    print(f"{'=' * 60}")

    with open(file_path, "r", encoding="utf-8") as f:
        leads = json.load(f)

    original_count = len(leads)
    print(f"Loaded {original_count} leads, max_retries={max_retries}\n")

    # Pre-filter: remove non-US/UAE leads first (fast, no API)
    filtered = []
    removed_country = 0
    for lead in leads:
        country = lead.get("country", "").strip().lower()
        if country in ALLOWED_COUNTRIES:
            filtered.append(lead)
        else:
            removed_country += 1
    if removed_country:
        print(f"Pre-filter: removed {removed_country} non-US/UAE leads\n")
    leads = filtered

    # Process each lead through validate-fix loop
    clean_leads = []
    removed = 0
    for i, lead in enumerate(leads):
        if process_lead(lead, max_retries, verbose):
            clean_leads.append(lead)
        else:
            removed += 1

        if (i + 1) % 100 == 0:
            print(f"  ... processed {i + 1}/{len(leads)} "
                  f"(kept {len(clean_leads)}, removed {removed + removed_country})")

    # Final summary
    total_removed = original_count - len(clean_leads)
    print(f"\n{'=' * 60}")
    print(f"Results:")
    print(f"  Original   : {original_count}")
    print(f"  Passed     : {len(clean_leads)}")
    print(f"  Removed    : {total_removed} "
          f"({removed_country} country + {removed} validation)")
    print(f"{'=' * 60}")

    if not dry_run:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(clean_leads, f, indent=2, ensure_ascii=False)
        print(f"Saved {len(clean_leads)} leads to {os.path.basename(file_path)}")
    else:
        print(f"[DRY RUN] Would save {len(clean_leads)} leads")


def main():
    parser = argparse.ArgumentParser(
        description="Validate-fix pipeline: simulate_submit + lead_updater in a loop"
    )
    parser.add_argument("--file", default="leads.json",
                        help="Target file in data/ folder (default: leads.json)")
    parser.add_argument("--max-retries", type=int, default=3,
                        help="Max fix attempts per lead (default: 3)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without saving")
    parser.add_argument("--quiet", action="store_true",
                        help="Only show summary, not per-lead output")
    args = parser.parse_args()

    if not os.getenv("OPENAI_API_KEY"):
        print("Warning: OPENAI_API_KEY not set. Role and city fixes will fail.")
        print("Set it: export OPENAI_API_KEY=sk-...")

    fp = os.path.join(DATA_DIR, args.file)
    if not os.path.exists(fp):
        print(f"File not found: {fp}")
        sys.exit(1)

    run_pipeline(fp, max_retries=args.max_retries, dry_run=args.dry_run,
                 verbose=not args.quiet)


if __name__ == "__main__":
    main()
