"""
Fix JN matches by scraping from individual player pages.
Each player page has ALL their matches (main + consolation).
Deduplicate to get the full match set.
"""
import asyncio
import json
import re
import sys
import io
from pathlib import Path
from playwright.async_api import async_playwright

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

TID = "A2DD0F5E-24A4-4875-B053-8F25F31AC357"
BASE = "https://www.tournamentsoftware.com"
OUT = Path(f"data/tournament_results/{TID}")


async def scrape_player_matches(page, tid, ts_pid, ts_to_usab, name_to_usab):
    """Scrape all matches from a player's page."""
    url = f"{BASE}/sport/player.aspx?id={tid}&player={ts_pid}"
    await page.goto(url, wait_until="networkidle", timeout=30000)

    text = await page.inner_text("body")
    html = await page.content()

    # Build name -> usab from links on this page
    for m in re.finditer(r'player\.aspx\?id=[^&]+&(?:amp;)?player=(\d+)"[^>]*>([^<]+)</a>', html):
        t_id = m.group(1)
        raw = m.group(2).strip()
        clean = re.sub(r"\s*\[\d+(?:/\d+)?\]", "", raw).strip()
        uid = ts_to_usab.get(t_id)
        if uid and clean:
            name_to_usab[clean] = uid

    # Find matches
    blocks = text.split("H2H")
    matches = []

    skip_words = {"Products", "Planner", "Helpdesk", "LiveScore", "Ranking", "CENTER",
                  "Social", "Privacy", "Disclaimer", "Cookies", "Download", "Branding",
                  "Tournamentsoftware", "Tournament", "League", "Box", "Visual",
                  "HELP", "helpdesk", "sales", "Our", "See", "Main Location",
                  "Statistics", "Total", "Singles", "Doubles"}

    for block in blocks[:-1]:
        block = block.strip()
        if not block or len(block) < 10:
            continue

        lines = [l.strip() for l in block.split("\n") if l.strip()]

        event = None
        round_name = None
        time_str = None
        is_walkover = "Walkover" in block

        player_lines = []
        w_index = None
        score_nums = []

        for line in lines:
            clean = re.sub(r"\s*\[\d+(?:/\d+)?\]", "", line).strip()

            # Event
            em = re.match(r"^(BS|GS|BD|GD|XD) U\d+$", line)
            if em:
                event = line
                continue

            # Round
            rm = re.match(r"^(Round of \d+|Round \d+|Quarter final|Semi final|"
                          r"Consolation final|Consolation semi final|Consolation quarter final|"
                          r"Consolation round of \d+|Final|Play-off|3rd/4th place)", line, re.IGNORECASE)
            if rm:
                round_name = rm.group(1)
                continue

            tm = re.match(r"^((?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) \d+/\d+/\d+ \d+:\d+ [AP]M)", line)
            if tm:
                time_str = tm.group(1)
                continue

            if re.match(r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun|January|February|March|April|May|June|July)", line):
                continue

            if clean == "W":
                w_index = len(player_lines)
                continue

            if clean in ("Walkover", "Bye", "", "Played", "Matches"):
                continue

            if re.match(r"^\d{1,2}$", clean):
                score_nums.append(int(clean))
                continue

            if any(sw in clean for sw in skip_words):
                continue

            # Player name
            if clean in name_to_usab:
                player_lines.append({"name": clean, "usab_id": name_to_usab[clean]})
            elif len(clean) > 2 and clean[0].isupper() and not re.match(r"^\d", clean):
                player_lines.append({"name": clean, "usab_id": None})

        if not event or not player_lines:
            continue

        # Game scores
        game_scores = []
        i = 0
        while i < len(score_nums) - 1:
            s1, s2 = score_nums[i], score_nums[i + 1]
            if s1 <= 30 and s2 <= 30 and (s1 >= 11 or s2 >= 11):
                game_scores.append([s1, s2])
                i += 2
            else:
                i += 1

        if not game_scores and not is_walkover:
            continue

        is_doubles = event.startswith(("BD", "GD", "XD"))
        team_size = 2 if is_doubles else 1

        # Split teams
        if w_index is not None and len(player_lines) >= team_size * 2:
            if is_doubles:
                if w_index == 2:
                    team1, team2, winner = player_lines[:2], player_lines[2:4], 1
                elif w_index >= 4:
                    team1, team2, winner = player_lines[:2], player_lines[2:4], 2
                else:
                    team1, team2 = player_lines[:2], player_lines[2:4]
                    t1w = sum(1 for s in game_scores if s[0] > s[1])
                    t2w = sum(1 for s in game_scores if s[1] > s[0])
                    winner = 1 if t1w > t2w else 2 if t2w > t1w else None
            else:
                if w_index == 1:
                    team1, team2, winner = [player_lines[0]], [player_lines[1]], 1
                elif w_index >= 2:
                    team1, team2, winner = [player_lines[0]], [player_lines[1]], 2
                else:
                    team1, team2, winner = [player_lines[0]], [player_lines[1]], None
        elif len(player_lines) >= team_size * 2:
            team1 = player_lines[:team_size]
            team2 = player_lines[team_size:team_size * 2]
            t1w = sum(1 for s in game_scores if s[0] > s[1])
            t2w = sum(1 for s in game_scores if s[1] > s[0])
            winner = 1 if t1w > t2w else 2 if t2w > t1w else None
        else:
            team1 = player_lines[:team_size]
            team2 = player_lines[team_size:]
            winner = 1 if is_walkover else None

        if is_walkover and winner is None:
            winner = 1

        # Score-based correction
        if game_scores and winner and not is_walkover:
            t1w = sum(1 for s in game_scores if s[0] > s[1])
            t2w = sum(1 for s in game_scores if s[1] > s[0])
            sw = 1 if t1w > t2w else 2 if t2w > t1w else None
            if sw and sw != winner:
                winner = sw

        bracket = "consolation" if round_name and "consolation" in round_name.lower() else "main"

        matches.append({
            "event": event,
            "round": round_name,
            "bracket": bracket,
            "team1": team1,
            "team2": team2,
            "scores": game_scores,
            "winner": winner,
            "walkover": is_walkover,
            "time": time_str,
        })

    return matches


async def main():
    players = json.loads((OUT / "players.json").read_text(encoding="utf-8"))
    ts_to_usab = {pid: p["usab_id"] for pid, p in players.items() if p.get("usab_id")}
    name_to_usab = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Accept cookies
        await page.goto(f"{BASE}/tournament/{TID}", wait_until="networkidle", timeout=30000)
        try:
            await page.click("button:has-text('Accept')", timeout=3000)
            await asyncio.sleep(0.5)
        except:
            pass

        all_matches = []
        total = len(players)
        for i, (ts_pid, pdata) in enumerate(sorted(players.items(), key=lambda x: int(x[0])), 1):
            matches = await scrape_player_matches(page, TID, ts_pid, ts_to_usab, name_to_usab)
            all_matches.extend(matches)
            if i % 50 == 0 or i == total:
                print(f"  [{i}/{total}] {len(all_matches)} matches so far")

        await browser.close()

    # Deduplicate: same event + round + same player set + same scores = same match
    seen = set()
    deduped = []
    for m in all_matches:
        pids = tuple(sorted(
            (p.get("usab_id") or p.get("name", "?"))
            for p in m["team1"] + m["team2"]
        ))
        scores_key = tuple(tuple(s) for s in m["scores"])
        key = (m["event"], m.get("round") or "", pids, scores_key, m.get("walkover", False))
        if key not in seen:
            seen.add(key)
            deduped.append(m)

    print(f"\nRaw: {len(all_matches)}, Deduped: {len(deduped)}")

    # Count Grace
    grace = [m for m in deduped if any(p.get("usab_id") == "440346" for p in m["team1"] + m["team2"])]
    print(f"Grace Cheng matches: {len(grace)}")
    for m in grace:
        t1 = "+".join(p["name"] for p in m["team1"])
        t2 = "+".join(p["name"] for p in m["team2"]) or "?"
        scores = ", ".join(f"{s[0]}-{s[1]}" for s in m["scores"])
        print(f"  {m['event']} {m['round']} [{m['bracket']}]: [{t1}] vs [{t2}] {scores}")

    # Save
    (OUT / "matches.json").write_text(json.dumps(deduped, indent=2, ensure_ascii=False), encoding="utf-8")
    t = json.loads((OUT / "tournament.json").read_text(encoding="utf-8"))
    t["matches"] = deduped
    (OUT / "tournament.json").write_text(json.dumps(t, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nSaved {len(deduped)} matches")


if __name__ == "__main__":
    asyncio.run(main())
