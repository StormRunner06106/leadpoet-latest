"""
Lead data updater - cleans leads*.json files to pass submit.py gateway validation.

Steps (run in order):
  1. Remove leads not based in United States or United Arab Emirates
  2. Fix first/last names (capitalize, strip bad chars, no spaces, no suffixes)
  3. Polish roles via OpenAI to meet submit.py check_role_sanity rules
  4. Fix city names via OpenAI to exact geo_lookup names (no "Greater", "Bay Area", etc.)
  5. Remove navigation/website links from descriptions

Usage:
    python data/lead_updater.py                   # process all leads*.json
    python data/lead_updater.py --file leads.json  # process single file
    python data/lead_updater.py --dry-run          # preview changes without saving

Requires: OPENAI_API_KEY environment variable set.
"""

import argparse
import glob
import json
import os
import re
import sys
import time

DATA_DIR = os.path.dirname(os.path.abspath(__file__))

try:
    from openai import OpenAI
except ImportError:
    print("openai package required: pip install openai")
    sys.exit(1)

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ============================================================
# Step 1: Remove non-US/UAE leads
# ============================================================

ALLOWED_COUNTRIES = {"united states", "united arab emirates", "us", "usa"}


def step1_filter_country(leads: list) -> list:
    """Remove leads whose country is not US or UAE."""
    filtered = []
    removed = 0
    for lead in leads:
        country = lead.get("country", "").strip().lower()
        if country in ALLOWED_COUNTRIES:
            filtered.append(lead)
        else:
            removed += 1
    if removed:
        print(f"  [Step 1] Removed {removed} non-US/UAE leads")
    else:
        print(f"  [Step 1] All leads are US/UAE - no removals")
    return filtered


# ============================================================
# Step 2: Fix first/last names
# ============================================================
# submit.py rules:
#   - No commas, periods, parentheses, brackets, digits
#   - No all-caps words 3+ chars (credentials: MBA, PhD, CPA)
#   - Blocklist titles/suffixes: jr, sr, dr, mr, mrs, phd, mba, rn, cpa, esq, etc.
#   - first != last (case-insensitive)
#   - first/last must not be all lowercase
#   - full_name must start with first and end with last
#   - No spaces in first or last name individually

_NAME_BAD_CHARS = re.compile(r'[,.\(\)\[\]\{\}0-9!@#$%^&*+=:;"\'<>?/\\|~`]')
_NAME_ALLCAPS = re.compile(r'\b[A-Z]{3,}\b')
_NAME_BLOCKLIST = {
    'ii', 'iii', 'iv', 'jr', 'sr', 'dr', 'mr', 'mrs', 'ms', 'prof',
    'phd', 'mba', 'rn', 'cpa', 'esq', 'dds', 'np',
    'lcsw', 'pmp', 'cfa', 'cfp', 'cissp', 'sphr', 'scp',
}


def _clean_name_part(name: str) -> str:
    """Clean a single name part (first or last)."""
    name = _NAME_BAD_CHARS.sub('', name).strip()

    words = name.split()
    cleaned = []
    for w in words:
        w_lower = w.rstrip(".'").lower()
        if w_lower in _NAME_BLOCKLIST:
            continue
        if _NAME_ALLCAPS.fullmatch(w) and len(w) >= 3:
            continue
        cleaned.append(w)

    if not cleaned:
        return name.strip()

    # Take only the first word (no spaces allowed in first or last)
    result = cleaned[0]

    # Capitalize first letter
    if result and result == result.lower():
        result = result[0].upper() + result[1:]

    return result


def step2_fix_names(leads: list) -> list:
    """Fix first/last/full_name to pass submit.py name validation."""
    fixed = 0
    for lead in leads:
        first_orig = lead.get("first", "").strip()
        last_orig = lead.get("last", "").strip()

        first = _clean_name_part(first_orig)
        last = _clean_name_part(last_orig)

        # Capitalize first letter if all lowercase
        if first and first == first.lower():
            first = first[0].upper() + first[1:]
        if last and last == last.lower():
            last = last[0].upper() + last[1:]

        # first and last must not be the same
        if first.lower() == last.lower() and first:
            continue  # skip - can't auto-fix

        full_name = f"{first} {last}".strip()

        changed = (first != first_orig or last != last_orig
                   or full_name != lead.get("full_name", ""))

        if changed:
            lead["first"] = first
            lead["last"] = last
            lead["full_name"] = full_name
            fixed += 1

    print(f"  [Step 2] Fixed {fixed} name(s)")
    return leads


# ============================================================
# Step 3: Polish roles via OpenAI
# ============================================================
# submit.py check_role_sanity rules (48 checks):
#   - 4-80 chars, must have letters, no mostly numbers
#   - No placeholders, repeated chars/words, scam patterns
#   - No URLs, emails, phone numbers, non-English chars, emojis
#   - No typos in common job words, no special chars at start/end
#   - No achievement statements, incomplete titles, company patterns
#   - No hiring markers, bio descriptions, taglines
#   - No degrees, pronouns, status statements, hashtags
#   - No generic standalone terms, certifications alone, skills alone
#   - No languages alone, years of experience, retired/aspiring
#   - No person's name, company name, city/state/country, industry in role
#   - No geographic location at end

ROLE_PROMPT = """You are a job title normalizer. Clean the given role to be a valid, concise professional job title.

Rules:
- Output ONLY the cleaned job title, nothing else
- 4-80 characters max
- Must be an actual job title (e.g. "Chief Financial Officer", "VP of Sales", "Software Engineer")
- Remove company names, person names, locations, industry names
- Remove credentials/degrees (MBA, PhD, CPA, etc.)
- Remove taglines, mission statements, bio text ("Helping companies...", "Passionate about...")
- Remove "at Company" or "in Location" suffixes
- Remove hiring markers ("We're hiring", "Open to work")
- Remove achievements ("Award-winning", "$1M+ revenue")
- Remove pronouns (he/him, she/her)
- Remove hashtags, URLs, emails, phone numbers, emojis
- Remove "Founder &" or "Co-Founder &" if followed by another valid title - keep both as "Co-Founder & CEO"
- If role is a degree (MBA, PhD), skill (Python, Excel), language (English), or certification (PMP, CPA) alone, return empty
- If role already looks like a clean job title, return it unchanged
- Do NOT invent a role - if the input is garbage, return empty string
- Use standard English title case"""


def _needs_role_fix(role: str, lead: dict) -> bool:
    """Quick check if a role likely needs OpenAI fixing."""
    if not role or len(role) < 4 or len(role) > 80:
        return True
    r = role.lower()

    if any(c in role for c in '%@#$^*[]{}|;\\`~<>?+'):
        return True
    if re.search(r'[,.\(\)\[\]0-9]', role) and not re.match(r'^[A-Za-z /&,.-]+$', role):
        return True
    if re.search(r'https?://|www\.|\.com|\.org|\.net', r):
        return True
    if re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', role):
        return True
    if '. ' in role and len(role) > 40:
        return True
    if role.count('.') > 1 or '!' in role:
        return True
    if re.search(r'\b(helping|passionate|dedicated|committed|empowering|driving)\b', r):
        return True
    if re.search(r'\b(student|retired|intern|volunteer|mba|phd)\b', r) and len(role.split()) <= 2:
        return True
    if re.search(r'^(he|she|they)\s*/\s*(him|her|them)', r):
        return True
    if re.search(r'#\w+', role):
        return True

    company = lead.get("business", "").lower()
    if company and len(company) > 3 and company in r:
        if f" at {company}" not in r:
            return True

    full_name = lead.get("full_name", "").lower()
    if full_name:
        for part in full_name.split():
            if len(part) > 2 and part in r:
                return True

    return False


def _fix_role_openai(role: str) -> str:
    """Call OpenAI to clean a role."""
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": ROLE_PROMPT},
                {"role": "user", "content": role},
            ],
            temperature=0,
            max_tokens=60,
        )
        return resp.choices[0].message.content.strip().strip('"').strip("'")
    except Exception as e:
        print(f"    OpenAI error for role '{role[:40]}': {e}")
        return role


def step3_polish_roles(leads: list, dry_run: bool = False) -> list:
    """Polish roles that would fail check_role_sanity."""
    fixed = 0
    for lead in leads:
        role = lead.get("role", "").strip()
        if not role:
            continue
        if not _needs_role_fix(role, lead):
            continue

        if dry_run:
            print(f"    [DRY] Would fix role: '{role[:60]}'")
            fixed += 1
            continue

        new_role = _fix_role_openai(role)
        if new_role and new_role != role:
            lead["role"] = new_role
            fixed += 1
            if fixed <= 10:
                print(f"    '{role[:40]}' -> '{new_role}'")
        time.sleep(0.1)

    print(f"  [Step 3] Fixed {fixed} role(s)")
    return leads


# ============================================================
# Step 4: Fix city names via OpenAI
# ============================================================
# submit.py validates city against geo_lookup.
# Common problems:
#   - "Greater Boston" -> "Boston"
#   - "San Francisco Bay Area" -> "San Francisco"
#   - "Greater Houston" -> "Houston"
#   - "Dallas-Fort Worth" -> "Dallas"
#   - "NYC" -> "New York"
#   - "LA" -> "Los Angeles"
#   - "Philly" -> "Philadelphia"
#   - "DMV Area" -> "Washington"
#   - County names like "Suffolk County" -> actual city

CITY_PROMPT = """You are a US city name normalizer. Convert the given city name to its exact, official city name that would appear in a US geographic database.

Rules:
- Output ONLY the exact city name, nothing else
- Remove prefixes: "Greater", "Metropolitan", "Metro", "Downtown"
- Remove suffixes: "Area", "Bay Area", "Metro Area", "Metropolitan Area", "Region"
- Remove "County" and county names - convert to the major city in that county
- Convert abbreviations: NYC->New York, LA->Los Angeles, SF->San Francisco, DC->Washington, Philly->Philadelphia
- Convert multi-city formats: "Dallas-Fort Worth"->Dallas, "Minneapolis-St. Paul"->Minneapolis
- If it's already an exact city name, return it unchanged
- If you cannot determine the exact city, return the input unchanged
- Use proper capitalization"""


def _needs_city_fix(city: str) -> bool:
    """Quick check if a city name likely needs fixing."""
    if not city:
        return False
    c = city.lower()
    triggers = [
        'greater ', 'metro ', 'metropolitan ',
        ' area', ' bay area', ' metro', ' region', ' county',
        'downtown ', 'nyc', ' dmv',
    ]
    if any(t in c for t in triggers):
        return True
    if c in ('la', 'sf', 'dc', 'nyc', 'philly', 'dmv'):
        return True
    if '-' in city and len(city.split('-')) == 2:
        return True
    return False


def _fix_city_openai(city: str, state: str) -> str:
    """Call OpenAI to normalize a city name."""
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": CITY_PROMPT},
                {"role": "user", "content": f"{city}, {state}" if state else city},
            ],
            temperature=0,
            max_tokens=30,
        )
        result = resp.choices[0].message.content.strip().strip('"').strip("'")
        # OpenAI might return "City, State" - take only the city part
        if ',' in result:
            result = result.split(',')[0].strip()
        return result
    except Exception as e:
        print(f"    OpenAI error for city '{city}': {e}")
        return city


def step4_fix_cities(leads: list, dry_run: bool = False) -> list:
    """Fix city names to exact names for geo_lookup validation."""
    fixed = 0
    for lead in leads:
        city = lead.get("city", "").strip()
        hq_city = lead.get("hq_city", "").strip()
        state = lead.get("state", "").strip()
        hq_state = lead.get("hq_state", "").strip()

        if _needs_city_fix(city):
            if dry_run:
                print(f"    [DRY] Would fix city: '{city}'")
                fixed += 1
            else:
                new_city = _fix_city_openai(city, state)
                if new_city and new_city != city:
                    lead["city"] = new_city
                    fixed += 1
                    if fixed <= 10:
                        print(f"    city: '{city}' -> '{new_city}'")
                time.sleep(0.1)

        if _needs_city_fix(hq_city):
            if dry_run:
                print(f"    [DRY] Would fix hq_city: '{hq_city}'")
                fixed += 1
            else:
                new_hq = _fix_city_openai(hq_city, hq_state)
                if new_hq and new_hq != hq_city:
                    lead["hq_city"] = new_hq
                    fixed += 1
                    if fixed <= 10:
                        print(f"    hq_city: '{hq_city}' -> '{new_hq}'")
                time.sleep(0.1)

    print(f"  [Step 4] Fixed {fixed} city name(s)")
    return leads


# ============================================================
# Step 5: Remove navigation links from descriptions
# ============================================================
# Pattern: "Website <url> External link for <name>" at end of descriptions

_DESC_NAV_PATTERN = re.compile(
    r'\s*Website\s+https?://\S+\s+External link for\s+.+$'
)


def step5_clean_descriptions(leads: list) -> list:
    """Remove trailing navigation/website links from descriptions."""
    fixed = 0
    for lead in leads:
        desc = lead.get("description", "")
        cleaned = _DESC_NAV_PATTERN.sub('', desc).strip()
        if cleaned != desc:
            lead["description"] = cleaned
            fixed += 1
    print(f"  [Step 5] Cleaned {fixed} description(s)")
    return leads


# ============================================================
# Main
# ============================================================

def process_file(file_path: str, dry_run: bool = False):
    """Run all steps on a single leads JSON file."""
    print(f"\nProcessing {os.path.basename(file_path)}...")

    with open(file_path, "r", encoding="utf-8") as f:
        leads = json.load(f)

    original_count = len(leads)
    print(f"  Loaded {original_count} leads")

    leads = step1_filter_country(leads)
    leads = step2_fix_names(leads)
    leads = step3_polish_roles(leads, dry_run=dry_run)
    leads = step4_fix_cities(leads, dry_run=dry_run)
    leads = step5_clean_descriptions(leads)

    if not dry_run:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(leads, f, indent=2, ensure_ascii=False)
        print(f"  Saved {len(leads)} leads (removed {original_count - len(leads)})")
    else:
        print(f"  [DRY RUN] Would save {len(leads)} leads (remove {original_count - len(leads)})")


def main():
    parser = argparse.ArgumentParser(description="Clean leads data to pass gateway validation")
    parser.add_argument("--file", help="Process a single file (e.g. leads.json)")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without saving")
    args = parser.parse_args()

    if not os.getenv("OPENAI_API_KEY"):
        print("Warning: OPENAI_API_KEY not set - Steps 3 and 4 (role/city fixes) will be skipped")

    if args.file:
        fp = os.path.join(DATA_DIR, args.file)
        if not os.path.exists(fp):
            print(f"File not found: {fp}")
            sys.exit(1)
        process_file(fp, dry_run=args.dry_run)
    else:
        for fp in sorted(glob.glob(os.path.join(DATA_DIR, "leads*.json"))):
            process_file(fp, dry_run=args.dry_run)

    print("\nDone.")


if __name__ == "__main__":
    main()
