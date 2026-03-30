"""Re-scrape tournaments that have incomplete matches (empty team1 or team2)."""
import os
os.environ["PYTHONUTF8"] = "1"

import asyncio
import json
import sys
from pathlib import Path
from playwright.async_api import async_playwright
from scrape_all_tournaments import scrape_tournament, BASE, DATA


def find_affected_tournaments():
    """Find tournament IDs with incomplete matches."""
    results_dir = Path("data/tournament_results")
    affected = []
    for tid in os.listdir(results_dir):
        matches_path = results_dir / tid / "matches.json"
        tourn_path = results_dir / tid / "tournament.json"
        if not matches_path.exists() or not tourn_path.exists():
            continue
        matches = json.loads(matches_path.read_text(encoding="utf-8"))
        tourn = json.loads(tourn_path.read_text(encoding="utf-8"))
        incomplete = sum(1 for m in matches if not m.get("team1") or not m.get("team2"))
        if incomplete:
            affected.append({"tid": tid, "name": tourn.get("name", tid), "incomplete": incomplete, "total": len(matches)})
    affected.sort(key=lambda x: -x["incomplete"])
    return affected


async def main():
    affected = find_affected_tournaments()
    print(f"Found {len(affected)} tournaments with incomplete matches")
    for t in affected:
        print(f"  {t['tid'][:8]}... {t['incomplete']}/{t['total']} incomplete - {t['name'][:60]}")

    print()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        # Accept cookies once
        page = await browser.new_page()
        await page.goto(f"{BASE}/", wait_until="networkidle", timeout=30000)
        try:
            await page.click("button:has-text('Accept')", timeout=3000)
            await asyncio.sleep(0.5)
        except:
            pass
        await page.close()

        for i, t in enumerate(affected, 1):
            print(f"\n[{i}/{len(affected)}] {t['name']} ({t['tid'][:8]}...)")
            # Remove existing data so it gets re-scraped
            out = DATA / "tournament_results" / t["tid"]
            for f in ["tournament.json", "matches.json", "players.json", "draws.json", "bracket_winners.json"]:
                fp = out / f
                if fp.exists():
                    fp.unlink()

            try:
                players, with_id, matches, winners, errors = await scrape_tournament(browser, t["tid"], t["name"])
                print(f"    -> {players} players ({with_id} ID'd), {matches} matches, {winners} winners, {errors} score errors")

                # Also update tournaments_combined
                combined_path = Path(f"data/tournaments_combined/{t['tid']}.json")
                tourn_path = out / "tournament.json"
                if tourn_path.exists() and combined_path.exists():
                    combined_path.write_text(tourn_path.read_text(encoding="utf-8"), encoding="utf-8")
                    print("    -> Updated tournaments_combined")
            except Exception as e:
                print(f"    !! ERROR: {e}")

        await browser.close()

    print("\nDone! Now re-run calculate_all_rankings.py to update rankings.")


if __name__ == "__main__":
    asyncio.run(main())
