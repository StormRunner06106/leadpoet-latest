"""
Remove trailing navigation/website links from lead descriptions.

Strips the "Website <url> External link for <name>" suffix that appears
at the end of most descriptions in leads*.json files.

Usage:
    python data/clean_descriptions.py
"""

import glob
import json
import os
import re

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
PATTERN = re.compile(r"\s*Website\s+https?://\S+\s+External link for\s+.+$")


def clean(description: str) -> str:
    return PATTERN.sub("", description).strip()


for fp in sorted(glob.glob(os.path.join(DATA_DIR, "leads*.json"))):
    with open(fp, "r", encoding="utf-8") as f:
        leads = json.load(f)

    changed = 0
    for lead in leads:
        original = lead.get("description", "")
        cleaned = clean(original)
        if cleaned != original:
            lead["description"] = cleaned
            changed += 1

    with open(fp, "w", encoding="utf-8") as f:
        json.dump(leads, f, indent=2, ensure_ascii=False)

    print(f"{os.path.basename(fp)}: cleaned {changed}/{len(leads)} descriptions")
