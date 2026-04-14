"""
Convert hq_country codes to full country names in leads*.json files.

Usage:
    python data/fix_country_names.py
"""

import glob
import json
import os

DATA_DIR = os.path.dirname(os.path.abspath(__file__))

COUNTRY_MAP = {
    "US": "United States",
    "AE": "United Arab Emirates",
}

for fp in sorted(glob.glob(os.path.join(DATA_DIR, "leads*.json"))):
    with open(fp, "r", encoding="utf-8") as f:
        leads = json.load(f)

    changed = 0
    for lead in leads:
        code = lead.get("hq_country", "")
        if code in COUNTRY_MAP:
            lead["hq_country"] = COUNTRY_MAP[code]
            changed += 1

    with open(fp, "w", encoding="utf-8") as f:
        json.dump(leads, f, indent=2, ensure_ascii=False)

    print(f"{os.path.basename(fp)}: converted {changed}/{len(leads)} country codes")
