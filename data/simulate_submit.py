"""
Simulate submit.py gateway validation for a lead from leads.json.

Runs all gateway-level checks in order, reports PASS/FAIL for each.
No network calls, no database, no wallet - pure local validation.

Usage:
    python data/simulate_submit.py <lead_id>
    python data/simulate_submit.py --all              # check all leads
    python data/simulate_submit.py --all --stop-on-fail

Checks (in submit.py order):
  1. Required fields
  2. Name sanity
  3. Role sanity (48 checks from role_patterns.json)
  4. Description sanity
  5. Industry taxonomy
  6. Contact location (country/state/city)
  7. HQ location (hq_country/hq_state/hq_city)
  8. Email domain vs website domain
  9. Employee count format
 10. Source provenance
 11. LinkedIn URL format
"""

import argparse
import glob
import json
import os
import re
import sys

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DATA_DIR)
sys.path.insert(0, PROJECT_ROOT)

# ── Load external configs ────────────────────────────────────
_rp_path = os.path.join(PROJECT_ROOT, "gateway", "api", "role_patterns.json")
with open(_rp_path, "r") as f:
    ROLE_PATTERNS = json.load(f)

ROLE_TYPO_DICT = {}
for _correct, _typos in ROLE_PATTERNS["typos"].items():
    for _t in _typos:
        ROLE_TYPO_DICT[_t.lower()] = _correct.lower()

ROLE_URL_PATTERNS = [r"https?://", r"\bwww\."]
for _tld in ROLE_PATTERNS["url_tlds"]:
    ROLE_URL_PATTERNS.append(rf"\b\w+\.{_tld}\b")

ROLE_NON_LATIN_RE = re.compile(ROLE_PATTERNS["non_latin_regex"])
ROLE_EMOJI_RE = re.compile(ROLE_PATTERNS["emoji_regex"])

from gateway.utils.geo_normalize import normalize_country, validate_location
from gateway.utils.industry_taxonomy import INDUSTRY_TAXONOMY

VALID_INDUSTRIES = set()
for _sub, _data in INDUSTRY_TAXONOMY.items():
    for _ind in _data.get("industries", []):
        VALID_INDUSTRIES.add(_ind)

# ── Free email domains ───────────────────────────────────────
FREE_EMAIL_DOMAINS = {
    'gmail.com', 'googlemail.com', 'yahoo.com', 'yahoo.co.uk', 'yahoo.fr',
    'yahoo.co.in', 'yahoo.co.jp', 'outlook.com', 'hotmail.com', 'live.com',
    'msn.com', 'aol.com', 'mail.com', 'protonmail.com', 'proton.me',
    'icloud.com', 'me.com', 'mac.com', 'zoho.com', 'yandex.com',
    'gmx.com', 'gmx.net', 'mail.ru', 'qq.com', '163.com', '126.com',
    'foxmail.com', 'sina.com', 'rediffmail.com', 'tutanota.com',
    'web.de', 't-online.de', 'wanadoo.fr', 'naver.com', 'daum.net',
    'hanmail.net', '139.com', 'sohu.com', 'aliyun.com',
}

VALID_EMPLOYEE_COUNTS = [
    "0-1", "2-10", "11-50", "51-200", "201-500",
    "501-1,000", "1,001-5,000", "5,001-10,000", "10,001+"
]

MULTI_PART_TLDS = frozenset({
    'co.uk', 'org.uk', 'ac.uk', 'gov.uk', 'com.au', 'net.au', 'org.au',
    'co.jp', 'or.jp', 'co.in', 'net.in', 'org.in', 'co.kr', 'com.br',
    'co.nz', 'co.za', 'com.mx', 'com.cn', 'com.tw', 'com.sg', 'co.il',
    'com.tr', 'co.id', 'com.ar', 'com.my', 'com.ph', 'co.th', 'com.vn',
    'com.ng', 'com.eg', 'com.pk', 'co.ke', 'com.ua', 'com.hk',
})


def _root_domain(domain: str) -> str:
    parts = domain.split('.')
    if len(parts) >= 3:
        last_two = '.'.join(parts[-2:])
        if last_two in MULTI_PART_TLDS:
            return '.'.join(parts[-3:])
    return '.'.join(parts[-2:]) if len(parts) >= 2 else domain


# ═══════════════════════════════════════════════════════════════
#  CHECK 1: Required fields
# ═══════════════════════════════════════════════════════════════
REQUIRED_FIELDS = [
    "business", "full_name", "first", "last", "email", "role",
    "website", "industry", "sub_industry", "country", "city",
    "linkedin", "company_linkedin", "source_url", "description",
    "employee_count",
]


def check_required_fields(lead: dict) -> tuple:
    missing = [f for f in REQUIRED_FIELDS
               if not lead.get(f) or (isinstance(lead.get(f), str) and not lead[f].strip())]
    if missing:
        return ("missing_required_fields", f"Missing: {', '.join(missing)}")
    return (None, None)


# ═══════════════════════════════════════════════════════════════
#  CHECK 2: Name sanity
# ═══════════════════════════════════════════════════════════════
_NAME_BAD = re.compile(r'[,.\(\)\[\]\{\}0-9]')
_NAME_CAPS = re.compile(r'\b[A-Z]{3,}\b')
_NAME_BLOCK = {'ii', 'iv', 'jr', 'sr', 'dr', 'mr', 'mrs', 'ms', 'prof',
               'phd', 'mba', 'rn', 'cpa', 'esq', 'dds', 'np',
               'lcsw', 'pmp', 'cfa', 'cfp', 'cissp', 'sphr', 'scp'}


def check_name(lead: dict) -> tuple:
    first = lead.get("first", "").strip()
    last = lead.get("last", "").strip()
    full = lead.get("full_name", "").strip()

    for name, val in [("first", first), ("last", last), ("full_name", full)]:
        if _NAME_BAD.search(val):
            return ("name_invalid_chars", f"'{name}' has invalid chars: '{val}'")

    for name, val in [("first", first), ("last", last), ("full_name", full)]:
        m = _NAME_CAPS.search(val)
        if m:
            return ("name_credential", f"'{name}' has credential '{m.group()}': '{val}'")

    for name, val in [("first", first), ("last", last), ("full_name", full)]:
        words = [w.rstrip(".'").lower() for w in val.split()]
        for w in words:
            if w in _NAME_BLOCK:
                return ("name_title_suffix", f"'{name}' has suffix '{w}': '{val}'")

    if first and last and first.lower() == last.lower():
        return ("name_duplicate", f"first '{first}' == last '{last}'")

    if first == first.lower():
        return ("name_lowercase", f"first '{first}' is all lowercase")
    if last == last.lower():
        return ("name_lowercase", f"last '{last}' is all lowercase")

    if full and first and last:
        if not (full == first or full.startswith(first + ' ')):
            return ("name_mismatch", f"full_name '{full}' doesn't start with first '{first}'")
        if not (full == last or full.endswith(' ' + last)):
            return ("name_mismatch", f"full_name '{full}' doesn't end with last '{last}'")

    return (None, None)


# ═══════════════════════════════════════════════════════════════
#  CHECK 3: Role sanity (all 48 checks)
# ═══════════════════════════════════════════════════════════════
def check_role(lead: dict) -> tuple:
    role_raw = lead.get("role", "").strip()
    full_name = lead.get("full_name", "").strip()
    company = lead.get("business", "").strip()
    city = lead.get("city", "").strip()
    state = lead.get("state", "").strip()
    country = lead.get("country", "").strip()
    industry = lead.get("industry", "").strip()

    r = role_raw
    rl = r.lower()
    th = ROLE_PATTERNS["thresholds"]
    letters = re.sub(r"[^a-zA-Z]", "", r)

    if len(r) < th["min_length"]:
        return ("role_too_short", f"{len(r)} chars < {th['min_length']}")
    if len(r) > th["max_length"]:
        return ("role_too_long", f"{len(r)} chars > {th['max_length']}")
    if len(r) > 80:
        return ("role_too_long_gaming", f"{len(r)} chars > 80")
    if not any(c.isalpha() for c in r):
        return ("role_no_letters", "No letters")
    if sum(c.isdigit() for c in r) > len(r) * th["max_digit_ratio"]:
        return ("role_mostly_numbers", "Mostly numbers")
    if rl in ROLE_PATTERNS["placeholders"]:
        return ("role_placeholder", "Placeholder")
    if re.search(r"(.)\1{3,}", r):
        return ("role_repeated_chars", "Repeated chars")

    wc = {}
    for w in rl.split():
        if len(w) > 1:
            wc[w] = wc.get(w, 0) + 1
    if any(c >= 3 for c in wc.values()):
        return ("role_repeated_words", "Word repeated 3+")

    for p in ROLE_PATTERNS["scam_patterns"]:
        if p in rl:
            return ("role_scam_pattern", f"Scam: '{p}'")
    if re.search(r"https?://|www\.|\.com/|\.org/|\.net/|\.io/", rl):
        return ("role_contains_url", "URL in role")
    if re.search(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", r):
        return ("role_contains_email", "Email in role")
    if re.search(r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b|\b\+\d{10,}", r):
        return ("role_contains_phone", "Phone in role")
    if ROLE_NON_LATIN_RE.findall(r):
        return ("role_non_english", "Non-English chars")
    if re.search(r"[àâäéèêëïîôùûüÿçñáíóúÀÂÄÉÈÊËÏÎÔÙÛÜŸÇÑÁÍÓÚßöÖ]", r):
        return ("role_invalid_format", "Accented chars")

    r_url = rl.replace(".net", "_NET_")
    for pat in ROLE_URL_PATTERNS:
        if re.search(pat, r_url):
            return ("role_contains_website", "Website domain in role")

    for w in re.findall(r"[a-zA-Z]+", rl):
        if w in ROLE_TYPO_DICT:
            return ("role_typo", f"'{w}' -> '{ROLE_TYPO_DICT[w]}'")

    if len(letters) < th["min_letters"]:
        return ("role_too_few_letters", f"{len(letters)} letters < {th['min_letters']}")
    if r and r[0] in ROLE_PATTERNS["special_chars"]:
        return ("role_starts_special_char", f"Starts with '{r[0]}'")
    if r and r[-1] in ROLE_PATTERNS["special_chars"]:
        return ("role_ends_special_char", f"Ends with '{r[-1]}'")

    for ch in r:
        if ch in '%@#$^*[]{}|;\\`~<>?+':
            return ("role_invalid_format", f"Bad char '{ch}'")
    if re.match(r"^\d+\s", r):
        return ("role_invalid_format", "Starts with number")
    if re.search(r"\d+\s*[xX]\b", r):
        return ("role_invalid_format", "Contains NxM pattern")
    if re.search(r"\b[Aa][Tt]\s+[A-Z][a-zA-Z]+", r):
        return ("role_invalid_format", "'At Company' in role")
    if re.search(r"\b[Ii][Nn]\s+[A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*\s*$", r):
        return ("role_invalid_format", "'In Location' at end")
    if re.search(r"\b401\s*\(?k\)?\b", rl):
        return ("role_invalid_format", "401k in role")

    for pfx in ['aba ', 'abm ', 'abl ', 'abh ', 'abs ', 'acca ', 'cma ']:
        if rl.startswith(pfx):
            return ("role_invalid_format", f"Starts with '{pfx.strip()}'")
    if rl.strip() in {'aba', 'abm', 'abl', 'abh', 'abs', 'acca', 'cma'}:
        return ("role_invalid_format", "Invalid abbreviation")

    for w in ['und', 'oder', 'geschäftsführer', 'directeur', 'directrice',
              'responsable', 'gérant', 'presidente', 'direttore', 'gerente']:
        if re.search(rf"\b{w}\b", rl):
            return ("role_invalid_format", f"Non-English word '{w}'")

    for p in ROLE_PATTERNS["achievement_patterns"]:
        if re.search(p, r, re.IGNORECASE):
            return ("role_achievement_statement", "Achievement statement")
    for p in ROLE_PATTERNS["incomplete_patterns"]:
        if re.search(p, rl.strip()):
            return ("role_incomplete_title", "Incomplete (ends with 'of')")
    for p in ROLE_PATTERNS["company_patterns"]:
        if re.search(p, r, re.IGNORECASE):
            return ("role_contains_company", "Company pattern")
    if ROLE_EMOJI_RE.search(r):
        return ("role_contains_emoji", "Emoji in role")
    for p in ROLE_PATTERNS["hiring_patterns"]:
        if re.search(p, rl):
            return ("role_hiring_marker", "Hiring marker")
    for p in ROLE_PATTERNS["bio_patterns"]:
        if re.search(p, rl):
            return ("role_bio_description", "Bio description")

    if len(r) > th["long_role_threshold"]:
        if not any(kw in rl for kw in ROLE_PATTERNS["job_keywords"]):
            return ("role_no_job_keywords", "Long role, no job keywords")

    if len(letters) >= 5:
        v = sum(1 for c in letters.lower() if c in 'aeiou')
        if v / len(letters) < th["min_vowel_ratio"]:
            return ("role_gibberish", "No vowels")

    if rl in ['student', 'mba', 'phd', 'intern', 'trainee', 'volunteer', 'retired']:
        return ("role_not_job_title", f"'{r}' not a job title")
    if rl.endswith(' intern') or rl.endswith(' trainee'):
        return ("role_intern_trainee", "Intern/trainee")
    if 'participant' in rl:
        return ("role_participant", "Participant")
    for w in ['enthusiast', 'hobbyist', 'lover', 'buff', 'aficionado', 'junkie', 'geek', 'nerd', 'addict']:
        if re.search(rf"\b{w}\b", rl):
            return ("role_invalid_format", f"Hobby word '{w}'")
    if rl.endswith(' in') or rl.endswith(' at') or rl.endswith(' for'):
        return ("role_truncated", "Ends with preposition")
    if re.search(r",\s*m\.?a\.?\s*in", rl) or re.search(r"juris|doctorate", rl):
        return ("role_has_degree", "Degree info")
    if '. ' in r and len(r) > 40:
        return ("role_marketing_tagline", "Marketing tagline")
    if r.count('.') > 1 or r.count('!') > 0:
        return ("role_excessive_punctuation", "Excessive punctuation")

    geo_end = (r"(?:,\s*|\s+[-–]\s+|\s+in\s+|\s+based\s+in\s+)"
               r"(?:New York|Los Angeles|San Francisco|Chicago|Houston|Phoenix|"
               r"Philadelphia|San Antonio|San Diego|Dallas|Austin|Denver|Seattle|"
               r"Boston|Miami|Atlanta|Portland|Nashville|Charlotte|Detroit|"
               r"London|Toronto|Sydney|Melbourne|Dubai|Singapore|Hong Kong|Tokyo|"
               r"Berlin|Paris|Amsterdam|Stockholm|Dublin|Zurich|Munich|"
               r"UK|Germany|France|Spain|Italy|Netherlands|"
               r"US|USA|Canada|Mexico|Brazil|Argentina)\s*$")
    if re.search(geo_end, r, re.IGNORECASE):
        return ("role_geo_at_end", "Geo location at end")

    if full_name:
        common = {"the", "and", "of", "in", "at", "for", "to", "a", "an", "is"}
        for part in full_name.lower().split():
            if len(part) > 2 and part not in common:
                if re.search(rf"\b{re.escape(part)}\b", rl):
                    return ("role_contains_name", f"Name '{part}' in role")

    if company:
        cl = company.lower().strip()
        if cl in rl and f" at {cl}" not in rl and f"@ {cl}" not in rl:
            return ("role_contains_company_name", f"Company '{company}' in role")
        cp = cl.split()
        if cp and len(cp[0]) > 3 and cp[0] in rl and f" at {cp[0]}" not in rl:
            return ("role_contains_company_name", f"Company word '{cp[0]}' in role")

    rls = rl.strip()
    if city and rls == city.lower().strip():
        return ("role_is_city", "Role = city name")
    if state and rls == state.lower().strip():
        return ("role_is_state", "Role = state name")
    if country and rls == country.lower().strip():
        return ("role_is_country", "Role = country name")
    if industry and rls == industry.lower().strip():
        return ("role_is_industry", "Role = industry name")

    taglines = [
        r"\bhelping\s+(you|companies|businesses|entrepreneurs|clients|organizations|people|teams|brands|startups|firms|individuals|others)",
        r"^i\s+help\b", r"\bi\s+am\s+a?\b", r"\bpassionate\s+about\b",
        r"\bhelping\s+to\b", r"\bhelping\s*$", r"\bdedicated\s+to\b",
        r"\bcommitted\s+to\b", r"\bempowering\b", r"\btransforming\b",
        r"\bdriving\b.*\bgrowth\b", r"\bmaking\s+a\s+difference\b",
        r"\bbuilding\b.*\bfuture\b", r"\bconnecting\b.*\bwith\b",
        r"\bserving\b.*\bclients\b", r"\bdelivering\b.*\bsolutions\b",
        r"\bfocused\s+on\b", r"\bspecializing\s+in\b.*\bhelp",
    ]
    for p in taglines:
        if re.search(p, rl):
            return ("role_is_tagline", "Tagline/mission statement")

    deg_ab = r"(mba|phd|msc|bsc|ma|ba|ms|bs|bba|bcom|mcom|llb|llm|md|mphil|dba|mph|mfa|med|edd)"
    deg_job = r"(director|manager|coordinator|advisor|adviser|recruiter|program|admissions|career|student\s+services|alumni|faculty|professor|instructor|teacher|coach|mentor|counselor|specialist|officer|lead|head|dean|chair)"
    dm = re.match(rf"^{deg_ab}\b", rl)
    if dm:
        after = rl[dm.end():].strip()
        if not after:
            return ("role_is_degree", "Just a degree")
        if not re.match(rf"^{deg_job}\b", after):
            return ("role_is_degree", "Degree + non-job")

    for p in [r"^bachelor'?s?\s+(degree|in|of)\b", r"^master'?s?\s+(degree|in|of)\b",
              r"^doctorate\s+(in|of)\b", r"^doctor\s+of\s+", r"^associate\s+degree\b"]:
        if re.search(p, rl):
            return ("role_is_degree", "Full degree name")

    for p in [r"^(he|she|they)\s*/\s*(him|her|them)$",
              r"^(he|she|they)\s*/\s*(him|her|them)\s*/\s*(his|hers|theirs)$",
              r"^\s*(he|she|they)\s*[/|]\s*(him|her|them)\s*$"]:
        if re.search(p, rl):
            return ("role_is_pronouns", "Pronouns")

    for p in [r"^open\s+to\s+work\b", r"^looking\s+for\s+(opportunities|work|job|new)\b",
              r"^seeking\s+(new\s+)?(opportunities|employment|work|job|role|position)\b",
              r"^actively\s+seeking\b", r"^available\s+for\s+(hire|work|opportunities)\b",
              r"^in\s+transition\b", r"^between\s+(jobs|roles|opportunities)\b",
              r"^job\s+seeker\b", r"^career\s+transition\b"]:
        if re.search(p, rl):
            return ("role_is_status", "Job-seeking status")

    if re.search(r"#\w+", r):
        return ("role_contains_hashtag", "Hashtag")
    if rls in {'professional', 'expert', 'freelancer', 'self-employed', 'self employed',
               'entrepreneur', 'leader', 'employee', 'worker', 'staff', 'member',
               'individual', 'person', 'human', 'adult'}:
        return ("role_too_generic", f"'{r}' too generic")

    for p in [r"^(cpa|pmp|cfa|cma|cisa|cissp|ccna|ccnp|aws|azure|gcp|scrum|csm|psm|safe|itil|prince2|six sigma|lean)\s*$",
              r"^certified\s+(public\s+accountant|project\s+manager|financial\s+analyst)\s*$"]:
        if re.search(p, rl):
            return ("role_is_certification", "Just a certification")

    if rls in {'python', 'java', 'javascript', 'typescript', 'react', 'angular', 'vue',
               'node', 'nodejs', 'sql', 'mysql', 'postgresql', 'mongodb', 'excel',
               'powerpoint', 'word', 'salesforce', 'sap', 'oracle', 'aws', 'azure', 'gcp',
               'docker', 'kubernetes', 'linux', 'windows', 'macos', 'ios', 'android',
               'html', 'css', 'php', 'ruby', 'go', 'rust', 'scala', 'kotlin', 'swift',
               'c++', 'c#', 'sales', 'marketing', 'finance', 'accounting', 'hr',
               'photoshop', 'illustrator', 'figma', 'sketch', 'tableau', 'power bi'}:
        return ("role_is_skill", f"'{r}' is a skill")

    if rls in {'english', 'spanish', 'french', 'german', 'italian', 'portuguese',
               'chinese', 'mandarin', 'cantonese', 'japanese', 'korean', 'arabic',
               'hindi', 'russian', 'dutch', 'swedish', 'norwegian', 'danish', 'finnish',
               'polish', 'turkish', 'hebrew', 'greek', 'thai', 'vietnamese', 'indonesian',
               'malay', 'tagalog', 'bengali', 'urdu', 'persian', 'farsi',
               'bilingual', 'trilingual', 'multilingual', 'polyglot'}:
        return ("role_is_language", f"'{r}' is a language")

    for p in [r"^\d+\+?\s*years?\s*(of\s+)?(experience|exp)\b", r"^\d+\+?\s*years?\s+in\s+",
              r"^over\s+\d+\s*years?\b", r"^experienced\s+in\b",
              r"^\d+\s*yrs?\s*(of\s+)?(experience|exp)\b"]:
        if re.search(p, rl):
            return ("role_is_experience", "Years of experience")

    for p in [r"^retired\s*$", r"^former\s*$", r"^ex-?\s*$", r"^previously\s*$", r"^past\s*$"]:
        if re.search(p, rl):
            return ("role_is_retired", "Retired/former")

    for p in [r"^aspiring\s+", r"^future\s+", r"^wannabe\s+",
              r"^soon\s+to\s+be\s+", r"^studying\s+to\s+(be|become)\s+"]:
        if re.search(p, rl):
            return ("role_is_aspiring", "Aspiring/future")

    return (None, None)


# ═══════════════════════════════════════════════════════════════
#  CHECK 4: Description sanity
# ═══════════════════════════════════════════════════════════════
def check_description(lead: dict) -> tuple:
    d = lead.get("description", "").strip()
    dl = d.lower()
    letters = re.sub(r"[^a-zA-Z]", "", d)

    if len(d) < 70:
        return ("desc_too_short", f"{len(d)} chars < 70")
    if len(d) > 2000:
        return ("desc_too_long", f"{len(d)} chars > 2000")
    if not any(c.isalpha() for c in d):
        return ("desc_no_letters", "No letters")
    if len(letters) < 50:
        return ("desc_too_few_letters", f"{len(letters)} letters < 50")
    if d.rstrip().endswith('...'):
        return ("desc_truncated", "Truncated (ends with ...)")
    if re.search(r'\d[\d,\.]*\s*followers?\s*(on\s*)?linkedin', dl):
        return ("desc_linkedin_followers", "LinkedIn follower count")
    for p in [r'\d[\d,\.]*\s*seguidores?\s*(en\s*)?linkedin',
              r'\d[\d,\.]*\s*abonnés?',
              r'\d[\d,\.]*\s*follower:?innen\s*(auf\s*)?linkedin',
              r'\d[\d,\.]*\s*sledujících', r'متابع.*linkedin', r'ผู้ติดตาม.*linkedin']:
        if re.search(p, dl, re.IGNORECASE):
            return ("desc_linkedin_foreign", "Non-English LinkedIn metadata")
    thai_re = re.compile(r'[\u0e00-\u0e7f]')
    if thai_re.search(d):
        lc = len(re.findall(r'[a-zA-Z]', d))
        tc = len(thai_re.findall(d))
        if lc > 20 and tc > 3:
            return ("desc_thai_mixed", "Thai mixed with English")
    for p in [r'report\s+this\s+company', r'close\s+menu', r'view\s+all\s*[\.;]?\s*about\s+us',
              r'follow\s*[·•]\s*report', r'external\s+(na\s+)?link\s+(for|para)',
              r'enlace\s+externo\s+para', r'laki\s+ng\s+kompanya',
              r'tamaño\s+de\s+la\s+empresa', r'webbplats:\s*http',
              r'nettsted:\s*http', r'sitio\s+web:\s*http', r'om\s+oss\.']:
        if re.search(p, dl):
            return ("desc_navigation_text", "Navigation/UI text")
    cjk_re = re.compile(r'[\u4e00-\u9fff\u3400-\u4dbf\u3040-\u309f\u30a0-\u30ff]')
    if cjk_re.search(d):
        lc = len(re.findall(r'[a-zA-Z]', d))
        if lc > 20 and len(cjk_re.findall(d)) > 0:
            return ("desc_garbled_unicode", "Garbled CJK Unicode")
    arabic_re = re.compile(r'[\u0600-\u06ff]')
    if arabic_re.search(d):
        lc = len(re.findall(r'[a-zA-Z]', d))
        if lc > 20 and len(arabic_re.findall(d)) > 3:
            return ("desc_arabic_mixed", "Arabic mixed with English")
    if len(letters) > 30:
        v = sum(1 for c in letters.lower() if c in 'aeiou')
        if v / len(letters) < 0.15:
            return ("desc_gibberish", "Gibberish (no vowels)")
    for ph in ["company description", "no description", "n/a", "none", "not available",
               "lorem ipsum", "test description", "placeholder", "description here", "enter description"]:
        if dl.strip() == ph or dl.startswith(ph + " "):
            return ("desc_placeholder", f"Placeholder: '{ph}'")
    if re.search(r'(.)\1{4,}', d):
        return ("desc_repeated_chars", "Repeated chars (spam)")
    if re.match(r'^https?://\S+$', d.strip()):
        return ("desc_just_url", "Just a URL")
    emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', d)
    if emails and sum(len(e) for e in emails) > len(d) * 0.3:
        return ("desc_mostly_email", "Mostly email")
    if d.startswith('|') or d.startswith(' |'):
        return ("desc_formatting_junk", "Starts with pipe")
    return (None, None)


# ═══════════════════════════════════════════════════════════════
#  CHECK 5: Industry taxonomy
# ═══════════════════════════════════════════════════════════════
def check_industry(lead: dict) -> tuple:
    ind = lead.get("industry", "").strip()
    sub = lead.get("sub_industry", "").strip()

    matched_sub = None
    if sub in INDUSTRY_TAXONOMY:
        matched_sub = sub
    else:
        for key in INDUSTRY_TAXONOMY:
            if key.lower() == sub.lower():
                matched_sub = key
                break
    if not matched_sub:
        return ("invalid_sub_industry", f"Sub-industry '{sub}' not in taxonomy")

    valid_inds = INDUSTRY_TAXONOMY[matched_sub].get("industries", [])
    if ind not in valid_inds:
        for vi in valid_inds:
            if vi.lower() == ind.lower():
                return (None, None)
        return ("invalid_industry_pairing", f"'{ind}' not valid for '{sub}'. Valid: {valid_inds}")
    return (None, None)


# ═══════════════════════════════════════════════════════════════
#  CHECK 6: Contact location
# ═══════════════════════════════════════════════════════════════
def check_contact_location(lead: dict) -> tuple:
    country_raw = lead.get("country", "").strip()
    state = lead.get("state", "").strip()
    city = lead.get("city", "").strip()

    country = normalize_country(country_raw) if country_raw else ""
    cl = country.lower()

    is_allowed = (cl == "united states" or
                  (cl == "united arab emirates" and city.lower().strip() == "dubai"))
    if not is_allowed:
        return ("invalid_region", f"Blocked: {city}/{state}/{country}")

    if cl == "united arab emirates" and state.strip():
        return ("invalid_region", f"UAE with state: {state}")

    ok, reason = validate_location(city, state, country)
    if not ok:
        return ("invalid_location", f"{reason}: {city}/{state}/{country}")
    return (None, None)


# ═══════════════════════════════════════════════════════════════
#  CHECK 7: HQ location
# ═══════════════════════════════════════════════════════════════
def check_hq_location(lead: dict) -> tuple:
    hc_raw = lead.get("hq_country", "").strip()
    hs = lead.get("hq_state", "").strip()
    hcity = lead.get("hq_city", "").strip()

    hc = normalize_country(hc_raw) if hc_raw else ""
    if hcity and hcity == hcity.lower():
        hcity = hcity.title()
    if hs and hs == hs.lower():
        hs = hs.title()

    hcity_l = hcity.lower() if hcity else ""
    hc_l = hc.lower() if hc else ""

    if hcity_l == "remote":
        if hs:
            return ("invalid_hq", f"Remote cannot have state: '{hs}'")
        if hc:
            return ("invalid_hq", f"Remote cannot have country: '{hc}'")
        return (None, None)

    if hc_l == "united arab emirates":
        if hcity_l not in {"dubai", "abu dhabi"}:
            return ("invalid_hq", f"UAE city must be Dubai/Abu Dhabi, got '{hcity}'")
        if hs:
            return ("invalid_hq", f"UAE cannot have state: '{hs}'")
        return (None, None)

    if hc_l == "united states":
        if not hs:
            return ("invalid_hq", "US HQ requires state")
        ok, reason = validate_location(hcity or "", hs, hc)
        if not ok:
            return ("invalid_hq", f"{reason}: {hcity}/{hs}/{hc}")
        return (None, None)

    if hcity or hs or hc:
        return ("invalid_hq", f"Blocked: {hcity}/{hs}/{hc}")

    return ("missing_hq_country", "HQ country is required")


# ═══════════════════════════════════════════════════════════════
#  CHECK 8: Email domain vs website
# ═══════════════════════════════════════════════════════════════
def check_email_domain(lead: dict) -> tuple:
    email = lead.get("email", "").strip().lower()
    website = lead.get("website", "").strip().lower()

    email_domain = email.split("@")[-1] if "@" in email else ""
    web_domain = re.sub(r'^https?://', '', website)
    web_domain = re.sub(r'^www\.', '', web_domain)
    web_domain = web_domain.split('/')[0].split('?')[0].split('#')[0].split(':')[0]

    if web_domain:
        web_root = _root_domain(web_domain)
        if web_domain != web_root:
            return ("website_is_subdomain", f"'{web_domain}' is subdomain, use '{web_root}'")

    if not email_domain or '.' not in email_domain:
        return ("invalid_email_format", f"Bad email format: '{email}'")

    if email_domain in FREE_EMAIL_DOMAINS:
        return ("free_email_domain", f"Free email: '{email_domain}'")

    if email_domain and web_domain:
        if _root_domain(email_domain) != _root_domain(web_domain):
            return ("email_domain_mismatch", f"Email '{email_domain}' != website '{web_domain}'")
    elif not web_domain and email_domain:
        return ("missing_website", f"No website to verify email domain '{email_domain}'")

    return (None, None)


# ═══════════════════════════════════════════════════════════════
#  CHECK 9: Employee count
# ═══════════════════════════════════════════════════════════════
def check_employee_count(lead: dict) -> tuple:
    ec = lead.get("employee_count", "").strip()
    if ec not in VALID_EMPLOYEE_COUNTS:
        return ("invalid_employee_count", f"'{ec}' not in {VALID_EMPLOYEE_COUNTS}")
    return (None, None)


# ═══════════════════════════════════════════════════════════════
#  CHECK 10: Source provenance
# ═══════════════════════════════════════════════════════════════
def check_source(lead: dict) -> tuple:
    st = lead.get("source_type", "").strip()
    su = lead.get("source_url", "").strip()

    if st == "proprietary_database" and su != "proprietary_database":
        return ("source_mismatch", f"source_type=proprietary_database but source_url='{su}'")
    if "linkedin" in su.lower():
        return ("linkedin_in_source_url", f"LinkedIn URL in source_url: '{su}'")
    return (None, None)


# ═══════════════════════════════════════════════════════════════
#  CHECK 11: LinkedIn URL format
# ═══════════════════════════════════════════════════════════════
def check_linkedin(lead: dict) -> tuple:
    li = (lead.get("linkedin", "") or "").strip().lower()
    cli = (lead.get("company_linkedin", "") or "").strip().lower()

    if li:
        if "linkedin.com" not in li:
            return ("invalid_linkedin_url", f"Not a LinkedIn URL: '{li}'")
        if "/in/" not in li:
            if "/company/" in li:
                return ("linkedin_url_wrong_type", f"Company URL in personal field: '{li}'")
            return ("linkedin_url_missing_profile", f"No /in/ in: '{li}'")

    if "linkedin.com/company/" not in cli:
        return ("invalid_company_linkedin", f"Company LinkedIn required with /company/. Got: '{cli}'")
    return (None, None)


# ═══════════════════════════════════════════════════════════════
#  Run all checks
# ═══════════════════════════════════════════════════════════════
ALL_CHECKS = [
    ("Required Fields", check_required_fields),
    ("Name Sanity", check_name),
    ("Role Sanity", check_role),
    ("Description Sanity", check_description),
    ("Industry Taxonomy", check_industry),
    ("Contact Location", check_contact_location),
    ("HQ Location", check_hq_location),
    ("Email/Domain", check_email_domain),
    ("Employee Count", check_employee_count),
    ("Source Provenance", check_source),
    ("LinkedIn URLs", check_linkedin),
]


def validate_lead(lead: dict, verbose: bool = True) -> list:
    """Run all checks. Returns list of (check_name, error_code, message) for failures."""
    failures = []
    for name, fn in ALL_CHECKS:
        code, msg = fn(lead)
        if code:
            failures.append((name, code, msg))
            if verbose:
                print(f"  FAIL [{name}] {code}: {msg}")
        elif verbose:
            print(f"  PASS [{name}]")
    return failures


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════
def load_lead_by_id(lead_id: int) -> dict | None:
    for fp in sorted(glob.glob(os.path.join(DATA_DIR, "leads*.json"))):
        with open(fp, "r", encoding="utf-8") as f:
            for lead in json.load(f):
                if lead.get("id") == lead_id:
                    return lead
    return None


def load_all_leads() -> list:
    all_leads = []
    for fp in sorted(glob.glob(os.path.join(DATA_DIR, "leads*.json"))):
        with open(fp, "r", encoding="utf-8") as f:
            all_leads.extend(json.load(f))
    return all_leads


def main():
    parser = argparse.ArgumentParser(description="Simulate submit.py gateway validation")
    parser.add_argument("lead_id", type=int, nargs="?", help="Lead id from leads.json")
    parser.add_argument("--all", action="store_true", help="Check all leads")
    parser.add_argument("--stop-on-fail", action="store_true", help="Stop on first failed lead")
    args = parser.parse_args()

    if args.all:
        leads = load_all_leads()
        passed = failed = 0
        fail_summary = {}
        for lead in leads:
            failures = validate_lead(lead, verbose=False)
            if failures:
                failed += 1
                for _, code, _ in failures:
                    fail_summary[code] = fail_summary.get(code, 0) + 1
                if args.stop_on_fail:
                    print(f"\nFailed lead #{lead.get('id')}: {lead.get('business')}")
                    validate_lead(lead, verbose=True)
                    break
            else:
                passed += 1

        print(f"\nResults: {passed} passed, {failed} failed out of {len(leads)}")
        if fail_summary:
            print("\nFailure breakdown:")
            for code, count in sorted(fail_summary.items(), key=lambda x: -x[1]):
                print(f"  {count:4d}  {code}")
        sys.exit(1 if failed else 0)

    elif args.lead_id is not None:
        lead = load_lead_by_id(args.lead_id)
        if not lead:
            print(f"Lead id={args.lead_id} not found.")
            sys.exit(1)

        print(f"Lead #{lead['id']}: {lead.get('business', '?')} ({lead.get('email', '?')})")
        print(f"{'=' * 60}")
        failures = validate_lead(lead)
        print(f"{'=' * 60}")
        print(f"Result: {'REJECTED' if failures else 'PASSED'} ({len(failures)} failure(s))")
        sys.exit(1 if failures else 0)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
