"""Re-scrape matches from draw pages for all tournaments.
Uses existing players.json and draws.json, only re-scrapes match data from draw pages.
Much faster than full re-scrape since it skips player page scraping.
"""
import asyncio
import json
import os
import sys
from pathlib import Path
from playwright.async_api import async_playwright

sys.path.insert(0, ".")
from scrape_all_tournaments import scrape_draw_matches, BASE

DATA = Path("data")


async def rescrape_tournament_draws(page, tid):
    """Re-scrape matches from draw pages for a single tournament."""
    out = DATA / "tournament_results" / tid
    players_path = out / "players.json"
    draws_path = out / "draws.json"
    tourn_path = out / "tournament.json"

    if not players_path.exists() or not draws_path.exists() or not tourn_path.exists():
        return None

    players = json.loads(players_path.read_text(encoding="utf-8"))
    draws = json.loads(draws_path.read_text(encoding="utf-8"))
    tournament = json.loads(tourn_path.read_text(encoding="utf-8"))

    # Build ts_player_id -> usab_id mapping
    ts_to_usab = {}
    for pid, p in players.items():
        if p.get("usab_id"):
            ts_to_usab[pid] = p["usab_id"]

    # Scrape all draws
    draw_matches = []
    for d in draws:
        try:
            dname, dmatches = await scrape_draw_matches(page, tid, d["draw_id"], d["name"], ts_to_usab)
            draw_matches.extend(dmatches)
        except Exception as e:
            pass

    if not draw_matches:
        return None

    # Deduplicate
    seen = set()
    deduped = []
    for m in draw_matches:
        pids = tuple(sorted((p.get("usab_id") or p.get("name", "?")) for p in m["team1"] + m["team2"]))
        skey = tuple(tuple(s) for s in m["scores"])
        key = (m.get("event", ""), m.get("round") or "", pids, skey, m.get("walkover", False))
        if key not in seen:
            seen.add(key)
            deduped.append(m)

    # Also keep player-page matches that aren't in draw data (walkovers, etc.)
    old_matches = tournament.get("matches", [])
    draw_player_sets = set()
    for m in deduped:
        pids = frozenset((p.get("usab_id") or p.get("name", "?")) for p in m["team1"] + m["team2"])
        draw_player_sets.add((m.get("event", ""), pids))

    supplemental = []
    for m in old_matches:
        pids = frozenset((p.get("usab_id") or p.get("name", "?")) for p in m.get("team1", []) + m.get("team2", []))
        if (m.get("event", ""), pids) not in draw_player_sets:
            supplemental.append(m)

    final_matches = deduped + supplemental

    # Update tournament.json and matches.json
    tournament["matches"] = final_matches
    tourn_path.write_text(json.dumps(tournament, ensure_ascii=False), encoding="utf-8")
    (out / "matches.json").write_text(json.dumps(final_matches, ensure_ascii=False), encoding="utf-8")

    # Update tournaments_combined
    combined_path = DATA / "tournaments_combined" / f"{tid}.json"
    if combined_path.exists():
        combined_path.write_text(json.dumps(tournament, ensure_ascii=False), encoding="utf-8")

    return len(deduped), len(supplemental)


async def main():
    tids = sorted(os.listdir(DATA / "tournament_results"))
    print(f"Re-scraping draw pages for {len(tids)} tournaments")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Accept cookies
        await page.goto(f"{BASE}/", wait_until="networkidle", timeout=30000)
        try:
            await page.click("button:has-text('Accept')", timeout=3000)
            await asyncio.sleep(0.5)
        except:
            pass

        done = 0
        for i, tid in enumerate(tids, 1):
            tourn_path = DATA / "tournament_results" / tid / "tournament.json"
            if not tourn_path.exists():
                continue
            t = json.loads(tourn_path.read_text(encoding="utf-8"))
            name = t.get("name", tid)[:55]

            try:
                result = await rescrape_tournament_draws(page, tid)
                done += 1
                if result:
                    draw_count, supp_count = result
                    print(f"[{done}/{len(tids)}] {name}: {draw_count} draw + {supp_count} supp", flush=True)
                else:
                    print(f"[{done}/{len(tids)}] {name}: skipped", flush=True)
            except Exception as e:
                done += 1
                print(f"[{done}/{len(tids)}] {name}: ERROR {str(e)[:60]}", flush=True)

        await browser.close()

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
