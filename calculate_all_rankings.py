"""Calculate rankings for all months, all disciplines, all age groups."""
import json
import sys
import io
from pathlib import Path
from datetime import datetime

sys.path.insert(0, ".")
# encoding handled by PYTHONUTF8=1 or -X utf8

from calculate_rankings import calculate_rankings

DATA = Path("data")
OUT = DATA / "calculated_rankings_monthly"
OUT.mkdir(exist_ok=True)

DISCIPLINES = ["BS", "GS", "BD", "GD", "XD"]
AGE_GROUPS = ["U11", "U13", "U15", "U17", "U19"]

# Generate all first-of-month dates
start = datetime(2021, 4, 1)
end = datetime(2026, 4, 1)
months = []
d = start
while d <= end:
    months.append(d.strftime("%Y-%m-%d"))
    if d.month == 12:
        d = d.replace(year=d.year + 1, month=1)
    else:
        d = d.replace(month=d.month + 1)

print(f"Calculating {len(months)} months x 25 combos = {len(months)*25} rankings")
print(f"Range: {months[0]} to {months[-1]}")
print()

# Also save a manifest of all available dates
manifest = {"dates": months, "disciplines": DISCIPLINES, "age_groups": AGE_GROUPS}

total = len(months)
for mi, month in enumerate(months, 1):
    out_file = OUT / f"{month}.json"
    if out_file.exists():
        print(f"[{mi}/{total}] {month}: SKIP (already exists)")
        continue

    print(f"[{mi}/{total}] {month}:", end=" ", flush=True)
    month_data = {}

    for disc in DISCIPLINES:
        for ag in AGE_GROUPS:
            key = f"{disc}_{ag}"
            try:
                rankings = calculate_rankings(disc, ag, month, top_n=4)
                month_data[key] = [
                    {
                        "rank": i + 1,
                        "usab_id": r["usab_id"],
                        "name": r["name"],
                        "yob": r.get("yob"),
                        "total_points": r["total_points"],
                        "top_results": r["top_results"][:4],
                    }
                    for i, r in enumerate(rankings[:128])
                ]
            except Exception as e:
                month_data[key] = []

    out_file.write_text(json.dumps(month_data, ensure_ascii=False), encoding="utf-8")
    total_players = sum(len(v) for v in month_data.values())
    print(f"{total_players} player entries")

manifest_path = OUT / "manifest.json"
manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
print(f"\nDone! Manifest saved to {manifest_path}")
