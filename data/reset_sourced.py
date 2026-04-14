import json
import glob
import os

data_dir = os.path.dirname(os.path.abspath(__file__))

for fp in sorted(glob.glob(os.path.join(data_dir, "leads*.json"))):
    with open(fp, "r", encoding="utf-8") as f:
        leads = json.load(f)

    for lead in leads:
        lead["is_sourced"] = False

    with open(fp, "w", encoding="utf-8") as f:
        json.dump(leads, f, indent=2, ensure_ascii=False)

    print(f"Reset {len(leads)} leads in {os.path.basename(fp)}")
