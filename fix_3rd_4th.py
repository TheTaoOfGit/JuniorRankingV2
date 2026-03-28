"""
Scrape 3rd/4th playoff matches from draw pages and merge into tournament data.
These matches are often missing from player-page scraping.
"""
import asyncio
import json
import re
import sys
import io
from pathlib import Path
from playwright.async_api import async_playwright

try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
except:
    pass

BASE = "https://www.tournamentsoftware.com"
DATA = Path("data")


async def scrape_3rd_4th_from_draw(page, tid, draw_id, draw_name, ts_to_usab):
    """Scrape the 3rd/4th match from a draw page."""
    url = f"{BASE}/sport/draw.aspx?id={tid}&draw={draw_id}"
    await page.goto(url, wait_until="networkidle", timeout=30000)

    html = await page.content()
    text = await page.inner_text("body")

    # Look for "Play-off 3/4" section in the text
    # Format in bracket:
    #   GS U13 - Play-off 3/4
    #   Club  Finals  Winner
    #   1  Player1
    #        Player2 (winner indicated by position in Winner column)
    #   2  Player2  score

    # Also check the match list section
    # Scroll to load matches
    for _ in range(5):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(0.3)

    text = await page.inner_text("body")

    # Find 3rd/4th match in match list
    blocks = text.split("H2H")
    for block in blocks:
        if "3rd/4th" not in block and "Play-off" not in block:
            continue

        lines = [l.strip() for l in block.split("\n") if l.strip()]

        # Extract players
        player_lines = []
        w_index = None
        score_nums = []
        found_context = False

        for line in lines:
            clean = re.sub(r"\s*\[\d+(?:/\d+)?\]", "", line).strip()
            if "3rd/4th" in line or "Play-off" in line:
                found_context = True
                continue
            if not found_context:
                continue
            if clean == "W":
                w_index = len(player_lines)
                continue
            if clean == "L":
                w_index = -(len(player_lines))
                continue
            if re.match(r"^\d{1,2}$", clean):
                score_nums.append(int(clean))
                continue
            if re.match(r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)", line):
                continue
            if any(skip in clean for skip in ["Products", "Planner", "Helpdesk", "LiveScore",
                                               "Ranking", "CENTER", "Social", "Privacy"]):
                continue
            if re.match(r"^.+ - [A-Z]?\d", clean) or re.match(r"^.+ - [A-Z]$", clean):
                continue
            if re.match(r"^(Capital|Main Location|Bintang|Arena|DFW|Synergy|Bellevue|Frisco|Shannon Pohl|Volleyball Gym|Badminton Gym)", clean):
                continue
            if len(clean) > 2 and clean[0].isupper() and not re.match(r"^\d", clean):
                # Try to get USAB ID from HTML links
                uid = None
                for m in re.finditer(r'player\.aspx\?id=[^&]+&(?:amp;)?player=(\d+)"[^>]*>([^<]+)</a>', html):
                    pname = re.sub(r"\s*\[\d+(?:/\d+)?\]", "", m.group(2).strip()).strip()
                    if pname == clean:
                        uid = ts_to_usab.get(m.group(1))
                        break
                player_lines.append({"name": clean, "usab_id": uid})

        if len(player_lines) < 2:
            continue

        # Build scores
        game_scores = []
        i = 0
        while i < len(score_nums) - 1:
            s1, s2 = score_nums[i], score_nums[i + 1]
            if s1 <= 30 and s2 <= 30 and (s1 >= 11 or s2 >= 11):
                game_scores.append([s1, s2])
                i += 2
            else:
                i += 1

        # Determine winner
        is_dbl = any(draw_name.startswith(x) for x in ("BD", "GD", "XD", "Doubles"))
        ts = 2 if is_dbl else 1

        if w_index is not None and w_index < 0:
            l_pos = -w_index
            if is_dbl:
                w_index = 4 if l_pos <= 2 else 2
            else:
                w_index = 2 if l_pos <= 1 else 1

        if w_index is not None and len(player_lines) >= ts * 2:
            if is_dbl:
                t1, t2 = player_lines[:2], player_lines[2:4]
                winner = 1 if w_index == 2 else 2 if w_index >= 4 else None
            else:
                t1, t2 = [player_lines[0]], [player_lines[1]]
                winner = 1 if w_index == 1 else 2 if w_index >= 2 else None
        else:
            t1 = player_lines[:ts]
            t2 = player_lines[ts:ts*2]
            winner = None

        # Score-based reorientation (winner-first on page)
        if game_scores and winner == 2:
            game_scores = [[s[1], s[0]] for s in game_scores]

        if winner is None and game_scores:
            tw1 = sum(1 for s in game_scores if s[0] > s[1])
            tw2 = sum(1 for s in game_scores if s[1] > s[0])
            winner = 1 if tw1 > tw2 else 2 if tw2 > tw1 else None

        event = draw_name.split(" - ")[0].strip()
        return {
            "event": event,
            "round": "3rd/4th place",
            "bracket": "main",
            "team1": t1,
            "team2": t2,
            "scores": game_scores,
            "winner": winner,
            "walkover": False,
            "time": None,
        }

    # Also try extracting from the bracket table (Play-off 3/4 section)
    # Look for player links in the 3rd/4th section
    playoff_section = re.search(r"Play-off 3/4(.+?)(?:Consolation|Matches|Our Products)", text, re.DOTALL)
    if playoff_section:
        section = playoff_section.group(1)
        # Find player names and W marker
        lines = [l.strip() for l in section.split("\n") if l.strip()]
        players = []
        winner_name = None

        for line in lines:
            clean = re.sub(r"\s*\[\d+(?:/\d+)?\]", "", line).strip()
            if re.match(r"^\d+\s", clean):
                clean = re.sub(r"^\d+\s+\S+\s+", "", clean).strip()  # Remove seed/club
            if len(clean) > 2 and clean[0].isupper() and not re.match(r"^\d", clean):
                # Check if this is in the Winner column (appears twice = winner)
                players.append(clean)

        if len(players) >= 2:
            # The player that appears most or last is typically the winner
            # In the bracket text, the winner's name appears in the Winner column
            from collections import Counter
            counts = Counter(players)
            # Most frequent = winner (appears in both Finals and Winner columns)
            winner_name = counts.most_common(1)[0][0] if counts else None

            unique_players = list(dict.fromkeys(players))[:2]
            if len(unique_players) == 2:
                t1_name = unique_players[0]
                t2_name = unique_players[1]

                # Get USAB IDs
                t1_uid = t2_uid = None
                for m2 in re.finditer(r'player\.aspx\?id=[^&]+&(?:amp;)?player=(\d+)"[^>]*>([^<]+)</a>', html):
                    pname = re.sub(r"\s*\[\d+(?:/\d+)?\]", "", m2.group(2).strip()).strip()
                    if pname == t1_name:
                        t1_uid = ts_to_usab.get(m2.group(1))
                    if pname == t2_name:
                        t2_uid = ts_to_usab.get(m2.group(1))

                is_dbl = any(draw_name.startswith(x) for x in ("BD", "GD", "XD", "Doubles"))
                event = draw_name.split(" - ")[0].strip()
                winner = 1 if winner_name == t1_name else 2 if winner_name == t2_name else None

                return {
                    "event": event,
                    "round": "3rd/4th place",
                    "bracket": "main",
                    "team1": [{"name": t1_name, "usab_id": t1_uid}],
                    "team2": [{"name": t2_name, "usab_id": t2_uid}],
                    "scores": [],
                    "winner": winner,
                    "walkover": False,
                    "time": None,
                }

    return None


async def main():
    results_dir = DATA / "tournament_results"
    global_map = {}

    # Build global name->uid
    for d in results_dir.iterdir():
        if not d.is_dir() or not (d / "players.json").exists():
            continue
        players = json.loads((d / "players.json").read_text(encoding="utf-8"))
        for pid, p in players.items():
            uid = p.get("usab_id")
            name = re.sub(r"\s*\(\d+\)\s*$", "", (p.get("name") or "").strip()).strip()
            if uid and name:
                global_map[name] = uid

    # Find tournaments with missing 3rd/4th matches
    tournaments_to_fix = []
    for d in sorted(results_dir.iterdir()):
        if not d.is_dir() or not (d / "tournament.json").exists():
            continue
        t = json.loads((d / "tournament.json").read_text(encoding="utf-8"))
        if len(t.get("players", {})) == 0:
            continue

        events = set(m.get("event") for m in t.get("matches", []))
        missing_events = []
        for ev in events:
            ev_matches = [m for m in t["matches"] if m.get("event") == ev]
            has_sf = any(m.get("round") == "Semi final" and m.get("bracket") != "consolation" for m in ev_matches)
            has_playoff = any("3rd/4th" in (m.get("round", "") or "") for m in ev_matches)
            if has_sf and not has_playoff:
                missing_events.append(ev)

        if missing_events:
            tournaments_to_fix.append((d.name, t.get("name", ""), missing_events))

    print(f"Tournaments with missing 3rd/4th: {len(tournaments_to_fix)}")
    total_events = sum(len(evs) for _, _, evs in tournaments_to_fix)
    print(f"Total events to check: {total_events}")

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

        fixed_total = 0
        for ti, (tid, tname, missing_events) in enumerate(tournaments_to_fix, 1):
            t_path = results_dir / tid / "tournament.json"
            t = json.loads(t_path.read_text(encoding="utf-8"))

            # Build ts_to_usab from players
            ts_to_usab = {}
            players = json.loads((results_dir / tid / "players.json").read_text(encoding="utf-8"))
            for pid, pl in players.items():
                if pl.get("usab_id"):
                    ts_to_usab[pid] = pl["usab_id"]

            # Get draw list
            draws = json.loads((results_dir / tid / "draws.json").read_text(encoding="utf-8"))
            event_to_draw = {}
            for d in draws:
                name = d["name"].split(" - ")[0].strip()
                if name not in event_to_draw:
                    event_to_draw[name] = d["draw_id"]

            fixed_count = 0
            for ev in missing_events:
                draw_id = event_to_draw.get(ev)
                if not draw_id:
                    continue

                try:
                    match = await scrape_3rd_4th_from_draw(page, tid, draw_id, ev, ts_to_usab)
                    if match:
                        # Map names to UIDs using global map
                        for team in [match["team1"], match["team2"]]:
                            for pl in team:
                                if not pl.get("usab_id") and pl.get("name"):
                                    pl["usab_id"] = global_map.get(pl["name"])

                        t["matches"].append(match)
                        fixed_count += 1
                except Exception as e:
                    pass

            if fixed_count > 0:
                t_path.write_text(json.dumps(t, ensure_ascii=False), encoding="utf-8")
                # Also update matches.json
                (results_dir / tid / "matches.json").write_text(
                    json.dumps(t["matches"], ensure_ascii=False), encoding="utf-8"
                )
                fixed_total += fixed_count

            if ti % 10 == 0 or ti == len(tournaments_to_fix):
                print(f"  [{ti}/{len(tournaments_to_fix)}] +{fixed_total} matches so far")

        await browser.close()

    print(f"\nDone! Added {fixed_total} 3rd/4th matches")


if __name__ == "__main__":
    asyncio.run(main())
