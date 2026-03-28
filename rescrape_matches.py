"""Rescrape all matches from draw pages with scroll-to-load and fixed score parsing."""
import asyncio
import json
import re
import sys
import io
from pathlib import Path
from playwright.async_api import async_playwright

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

TID = "5779DD58-5F08-4D64-A092-B41478B07A0A"
BASE = "https://www.tournamentsoftware.com"
OUT = Path(f"data/tournament_results/{TID}")


async def scrape_draw(page, tid, draw_id, draw_name, ts_to_usab):
    url = f"{BASE}/sport/draw.aspx?id={tid}&draw={draw_id}"
    await page.goto(url, wait_until="networkidle", timeout=30000)

    # Scroll to load lazy match section
    for _ in range(3):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(1)

    text = await (await page.query_selector("body")).inner_text()
    html = await page.content()

    # Build name -> usab_id from player links
    name_to_usab = {}
    for m in re.finditer(
        r'player\.aspx\?id=[^&]+&(?:amp;)?player=(\d+)"[^>]*>([^<]+)</a>', html
    ):
        ts_id = m.group(1)
        raw_name = m.group(2).strip()
        clean = re.sub(r"\s*\[\d+(?:/\d+)?\]", "", raw_name).strip()
        usab_id = ts_to_usab.get(ts_id)
        if usab_id and clean:
            name_to_usab[clean] = usab_id

    is_doubles = any(draw_name.startswith(x) for x in ("BD", "GD", "XD", "Doubles"))

    # Find matches section
    matches_idx = text.find("Matches\nPlayed")
    if matches_idx < 0:
        matches_idx = text.rfind("Matches\n")
    if matches_idx < 0:
        return draw_name, []

    match_text = text[matches_idx:]
    blocks = match_text.split("H2H")
    matches = []

    for block in blocks[:-1]:
        block = block.strip()
        if not block or len(block) < 10:
            continue

        lines = [l.strip() for l in block.split("\n") if l.strip()]

        round_name = None
        time_str = None
        is_walkover = "Walkover" in block

        skip_patterns = [
            r"^(Round of \d+|Round \d+|Quarter final|Semi final|Final|Play-off|Consolation|3rd/4th place)",
            r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)",
            r"^(January|February|March|April|May|June|July|August|September|October|November|December)",
            r"^Bintang",
            r"^Played$",
            r"^Matches$",
        ]

        player_lines = []
        w_index = None
        score_nums = []

        for line in lines:
            clean = re.sub(r"\s*\[\d+(?:/\d+)?\]", "", line).strip()

            # Round
            rm = re.match(
                r"^(Round of \d+|Round \d+|Quarter final|Semi final|Final|Play-off|Consolation|3rd/4th place)",
                line,
            )
            if rm:
                round_name = rm.group(1)
                continue

            # Time
            tm = re.match(
                r"^((?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) \d+/\d+/\d+ \d+:\d+ [AP]M)", line
            )
            if tm:
                time_str = tm.group(1)
                continue

            # Skip non-player lines
            if any(re.match(p, line) for p in skip_patterns):
                continue

            if clean == "W":
                w_index = len(player_lines)
                continue

            if clean in ("Walkover", "Bye", ""):
                continue

            # Pure number = score
            if re.match(r"^\d{1,2}$", clean):
                score_nums.append(int(clean))
                continue

            # Player name
            if clean in name_to_usab:
                player_lines.append({"name": clean, "usab_id": name_to_usab[clean]})
            elif (
                len(clean) > 2
                and clean[0].isupper()
                and not re.match(r"^\d", clean)
                and "Products" not in clean
                and "Planner" not in clean
                and "Helpdesk" not in clean
                and "LiveScore" not in clean
                and "Ranking" not in clean
                and "CENTER" not in clean
                and "Social" not in clean
                and "Privacy" not in clean
                and "Disclaimer" not in clean
                and "Cookies" not in clean
                and "Download" not in clean
            ):
                player_lines.append({"name": clean, "usab_id": None})

        # Build game scores from consecutive number pairs
        game_scores = []
        i = 0
        while i < len(score_nums) - 1:
            s1, s2 = score_nums[i], score_nums[i + 1]
            if s1 <= 30 and s2 <= 30 and (s1 >= 11 or s2 >= 11):
                game_scores.append([s1, s2])
                i += 2
            else:
                i += 1

        if not player_lines or (not game_scores and not is_walkover):
            continue

        # Split into teams and determine winner
        # Text format:
        #   Singles: Player1\nW\nPlayer2\nscores  (W after winner, 1 player)
        #            Player1\nPlayer2\nW\nscores  (W after winner = Player2)
        #   Doubles: P1a\nP1b\nP2a\nP2b\nW\nscores  (W after all 4, last team won)
        #            P1a\nP1b\nW\nP2a\nP2b\nscores  (W after first team, first team won)
        team_size = 2 if is_doubles else 1

        if w_index is not None and len(player_lines) >= team_size * 2:
            if is_doubles:
                if w_index == 2:
                    # W after first pair: first pair won
                    team1 = player_lines[:2]
                    team2 = player_lines[2:4]
                    winner = 1
                elif w_index >= 4:
                    # W after all 4: last pair (team2) won
                    team1 = player_lines[:2]
                    team2 = player_lines[2:4]
                    winner = 2
                else:
                    # Unusual position, split evenly, use scores
                    team1 = player_lines[:2]
                    team2 = player_lines[2:4]
                    t1w = sum(1 for s in game_scores if s[0] > s[1])
                    t2w = sum(1 for s in game_scores if s[1] > s[0])
                    winner = 1 if t1w > t2w else 2 if t2w > t1w else None
            else:
                if w_index == 1:
                    # W after first player: first player won
                    team1 = [player_lines[0]]
                    team2 = [player_lines[1]] if len(player_lines) > 1 else []
                    winner = 1
                elif w_index >= 2:
                    # W after second player: second player won
                    team1 = [player_lines[0]]
                    team2 = [player_lines[1]]
                    winner = 2
                else:
                    team1 = [player_lines[0]]
                    team2 = [player_lines[1]] if len(player_lines) > 1 else []
                    winner = None
        elif w_index is not None and len(player_lines) < team_size * 2:
            # Bye/walkover with W marker
            team1 = player_lines[:team_size]
            team2 = player_lines[team_size:]
            winner = 1
        elif len(player_lines) >= team_size * 2:
            # No W marker, split evenly, use scores
            team1 = player_lines[:team_size]
            team2 = player_lines[team_size : team_size * 2]
            t1w = sum(1 for s in game_scores if s[0] > s[1])
            t2w = sum(1 for s in game_scores if s[1] > s[0])
            winner = 1 if t1w > t2w else 2 if t2w > t1w else None
        else:
            # Not enough players (bye, walkover, etc.)
            team1 = player_lines[:team_size]
            team2 = player_lines[team_size:]
            winner = 1 if is_walkover else None

        if is_walkover and winner is None:
            winner = 1

        # POST-VALIDATION: if scores contradict winner, trust scores
        if game_scores and winner and not is_walkover:
            t1w = sum(1 for s in game_scores if s[0] > s[1])
            t2w = sum(1 for s in game_scores if s[1] > s[0])
            score_winner = 1 if t1w > t2w else 2 if t2w > t1w else None
            if score_winner and score_winner != winner:
                winner = score_winner

        event = draw_name.split(" - ")[0].strip()
        matches.append(
            {
                "event": event,
                "draw": draw_name,
                "round": round_name,
                "team1": team1,
                "team2": team2,
                "scores": game_scores,
                "winner": winner,
                "walkover": is_walkover,
                "time": time_str,
            }
        )

    return draw_name, matches


async def main():
    players = json.loads((OUT / "players.json").read_text(encoding="utf-8"))
    draws = json.loads((OUT / "draws.json").read_text(encoding="utf-8"))

    ts_to_usab = {}
    for pid, p in players.items():
        if p.get("usab_id"):
            ts_to_usab[pid] = p["usab_id"]

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Accept cookies once
        await page.goto(
            f"{BASE}/sport/draws.aspx?id={TID}",
            wait_until="networkidle",
            timeout=30000,
        )
        try:
            await page.click("button:has-text('Accept')", timeout=3000)
            await asyncio.sleep(0.5)
        except:
            pass

        all_matches = []
        for d in draws:
            name, matches = await scrape_draw(
                page, TID, d["draw_id"], d["name"], ts_to_usab
            )
            all_matches.extend(matches)
            print(f"  {name}: {len(matches)} matches")

        await browser.close()

    # Save
    (OUT / "matches.json").write_text(
        json.dumps(all_matches, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    t = json.loads((OUT / "tournament.json").read_text(encoding="utf-8"))
    t["matches"] = all_matches
    (OUT / "tournament.json").write_text(
        json.dumps(t, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    print(f"\nTotal: {len(all_matches)} matches")

    # Verify Grace vs Shirin
    print("\n=== Grace Cheng matches ===")
    for m in all_matches:
        all_p = m.get("team1", []) + m.get("team2", [])
        if any(p.get("usab_id") == "440346" for p in all_p):
            t1 = " + ".join(p["name"] for p in m["team1"])
            t2 = " + ".join(p["name"] for p in m["team2"]) or "Bye"
            w = "T1 wins" if m.get("winner") == 1 else "T2 wins" if m.get("winner") == 2 else "?"
            scores = ", ".join(f"{s[0]}-{s[1]}" for s in m["scores"])
            print(f"  {m['event']} {m['round']}: [{t1}] vs [{t2}] {scores} ({w})")


if __name__ == "__main__":
    asyncio.run(main())
