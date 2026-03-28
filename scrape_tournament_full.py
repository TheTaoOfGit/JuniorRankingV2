"""
Full tournament scraper: players, draws, matches, with validation.
Usage: python scrape_tournament_full.py <tournament_id>
"""

import asyncio
import json
import re
import sys
import io
from pathlib import Path
from playwright.async_api import async_playwright

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
BASE = "https://www.tournamentsoftware.com"


async def accept_cookies(page):
    try:
        await page.click("button:has-text('Accept')", timeout=3000)
        await asyncio.sleep(0.5)
    except:
        pass


async def scrape_overview(page, tid):
    """Get tournament name, dates, venue from overview page."""
    await page.goto(f"{BASE}/tournament/{tid}", wait_until="networkidle", timeout=60000)
    await accept_cookies(page)
    await page.wait_for_selector("body", timeout=10000)
    text = await page.inner_text("body")

    name = ""
    m = re.search(r"\n(.+CHAMPIONSHIPS|.+SELECTION.+|.+NATIONAL.+)\n", text)
    if m:
        name = m.group(1).strip()
    else:
        # Try first big heading
        lines = [l.strip() for l in text.split("\n") if l.strip()]
        for l in lines:
            if "USA Badminton" not in l and len(l) > 20 and l.isupper():
                name = l
                break

    dates = ""
    dm = re.search(r"((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d+ to (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d+)", text)
    if dm:
        dates = dm.group(1)

    venue = ""
    vm = re.search(r"ROUTE\s*\n(.+?)(?:\n|$)", text)
    if vm:
        venue = vm.group(1).strip()

    # Get tournament day URLs from Matches page
    await page.goto(f"{BASE}/tournament/{tid}/Matches", wait_until="networkidle", timeout=30000)
    html = await page.content()
    day_params = re.findall(r'Day=(\d{4}-\d{2}-\d{2})', html)
    day_dates = sorted(set(day_params))

    return {"name": name, "dates": dates, "venue": venue, "day_dates": day_dates}


async def scrape_draws(page, tid):
    """Get all draw IDs and names."""
    await page.goto(f"{BASE}/sport/draws.aspx?id={tid}", wait_until="networkidle", timeout=30000)
    draws = []
    links = await page.query_selector_all("a")
    for link in links:
        href = await link.get_attribute("href") or ""
        txt = (await link.inner_text()).strip()
        if "draw=" in href and txt:
            m = re.search(r"draw=(\d+)", href)
            if m:
                draws.append({"draw_id": m.group(1), "name": txt})
    return draws


async def scrape_players(page, tid):
    """Scrape all players with USAB IDs from draw pages + individual player pages."""
    # Collect player IDs from all draw pages
    draws_url = f"{BASE}/sport/draws.aspx?id={tid}"
    await page.goto(draws_url, wait_until="networkidle", timeout=30000)

    # Get all draw links
    html = await page.content()
    draw_ids = set(re.findall(r"draw=(\d+)", html))

    all_pids = set()
    for did in draw_ids:
        await page.goto(f"{BASE}/sport/draw.aspx?id={tid}&draw={did}", wait_until="networkidle", timeout=30000)
        h = await page.content()
        pids = set(re.findall(r"player=(\d+)", h))
        all_pids.update(pids)

    print(f"  Found {len(all_pids)} unique player IDs from draws")

    # Scrape each player page
    players = {}
    total = len(all_pids)
    for i, pid in enumerate(sorted(all_pids, key=int), 1):
        await page.goto(f"{BASE}/sport/player.aspx?id={tid}&player={pid}", wait_until="networkidle", timeout=30000)
        text = await (await page.query_selector("body")).inner_text()

        usab_id = None
        name = None
        m = re.search(r"\((\d{5,7})\)", text)
        if m:
            usab_id = m.group(1)

        # Extract name - look for "XX\nActualName" pattern before the USAB ID
        nm = re.search(r"\n([A-Z]{1,2})\n(.+?)(?:\n|$)", text)
        if nm:
            name = nm.group(2).strip()

        events = re.findall(r"((?:BS|GS|BD|GD|XD|Doubles|Shonai [BG]S) U\d+)", text)
        events = list(dict.fromkeys(events))

        players[pid] = {
            "ts_player_id": pid,
            "usab_id": usab_id,
            "name": name,
            "events": events,
        }

        if i % 50 == 0 or i == total:
            print(f"    [{i}/{total}] Last: {name} ({usab_id})")

    return players


async def scrape_draw_matches(page, tid, draw_id, draw_name, ts_to_usab):
    """Scrape all matches from a single draw page."""
    url = f"{BASE}/sport/draw.aspx?id={tid}&draw={draw_id}"
    await page.goto(url, wait_until="networkidle", timeout=30000)

    # Scroll aggressively to load lazy match section (large draws need many scrolls)
    prev_height = 0
    for _ in range(30):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(0.5)
        height = await page.evaluate("document.body.scrollHeight")
        if height == prev_height:
            break
        prev_height = height

    text = await (await page.query_selector("body")).inner_text()
    html = await page.content()

    # Build name -> usab_id from player links
    name_to_usab = {}
    for m in re.finditer(r"player\.aspx\?id=[^&]+&(?:amp;)?player=(\d+)\"[^>]*>([^<]+)</a>", html):
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

    # Track whether we're in the consolation section
    in_consolation = False

    skip_words = {"Products", "Planner", "Helpdesk", "LiveScore", "Ranking", "CENTER",
                  "Social", "Privacy", "Disclaimer", "Cookies", "Download", "Branding",
                  "Tournamentsoftware", "Tournament", "League", "Box", "Visual",
                  "HELP", "helpdesk", "sales", "Our", "See"}

    for block in blocks[:-1]:
        block = block.strip()
        if not block or len(block) < 10:
            continue

        # Detect consolation section
        if "Consolation" in block and ("Round" in block or "final" in block.lower()):
            in_consolation = True

        lines = [l.strip() for l in block.split("\n") if l.strip()]

        round_name = None
        time_str = None
        is_walkover = "Walkover" in block

        player_lines = []
        w_index = None
        score_nums = []

        for line in lines:
            clean = re.sub(r"\s*\[\d+(?:/\d+)?\]", "", line).strip()

            rm = re.match(r"^(Round of \d+|Round \d+|Quarter final|Semi final|Consolation final|Consolation semi final|Consolation quarter final|Final|Play-off|Consolation|3rd/4th place)", line, re.IGNORECASE)
            if rm:
                round_name = rm.group(1)
                continue

            tm = re.match(r"^((?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) \d+/\d+/\d+ \d+:\d+ [AP]M)", line)
            if tm:
                time_str = tm.group(1)
                continue

            if re.match(r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun|January|February|March|April|May|June|July|August|September|October|November|December)", line):
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

            if re.match(r"^(DFW|Bintang|Arena|Synergy|Capital|Bellevue|Frisco)", clean):
                continue  # venue names

            if clean in name_to_usab:
                player_lines.append({"name": clean, "usab_id": name_to_usab[clean]})
            elif len(clean) > 2 and clean[0].isupper() and not re.match(r"^\d", clean):
                player_lines.append({"name": clean, "usab_id": None})

        # Build game scores
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

        # Split into teams
        team_size = 2 if is_doubles else 1

        if w_index is not None and len(player_lines) >= team_size * 2:
            if is_doubles:
                if w_index == 2:
                    team1 = player_lines[:2]
                    team2 = player_lines[2:4]
                    winner = 1
                elif w_index >= 4:
                    team1 = player_lines[:2]
                    team2 = player_lines[2:4]
                    winner = 2
                else:
                    team1 = player_lines[:2]
                    team2 = player_lines[2:4]
                    t1w = sum(1 for s in game_scores if s[0] > s[1])
                    t2w = sum(1 for s in game_scores if s[1] > s[0])
                    winner = 1 if t1w > t2w else 2 if t2w > t1w else None
            else:
                if w_index == 1:
                    team1 = [player_lines[0]]
                    team2 = [player_lines[1]]
                    winner = 1
                elif w_index >= 2:
                    team1 = [player_lines[0]]
                    team2 = [player_lines[1]]
                    winner = 2
                else:
                    team1 = [player_lines[0]]
                    team2 = [player_lines[1]]
                    winner = None
        elif w_index is not None and len(player_lines) < team_size * 2:
            team1 = player_lines[:team_size]
            team2 = player_lines[team_size:]
            winner = 1
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

        # POST-VALIDATION: if scores contradict winner, trust scores
        if game_scores and winner and not is_walkover:
            t1w = sum(1 for s in game_scores if s[0] > s[1])
            t2w = sum(1 for s in game_scores if s[1] > s[0])
            score_winner = 1 if t1w > t2w else 2 if t2w > t1w else None
            if score_winner and score_winner != winner:
                winner = score_winner

        event = draw_name.split(" - ")[0].strip()
        bracket_type = "consolation" if in_consolation else "main"
        if round_name and "3rd/4th" in round_name:
            bracket_type = "playoff"
        matches.append({
            "event": event,
            "draw": draw_name,
            "round": round_name,
            "bracket": bracket_type,
            "team1": team1,
            "team2": team2,
            "scores": game_scores,
            "winner": winner,
            "walkover": is_walkover,
            "time": time_str,
        })

    return draw_name, matches


async def get_bracket_winners(page, tid, draws, ts_to_usab):
    """Extract winners from bracket Winner column."""
    winners = {}
    for d in draws:
        if "Group" in d["name"]:
            continue
        await page.goto(f"{BASE}/sport/draw.aspx?id={tid}&draw={d['draw_id']}", wait_until="networkidle", timeout=30000)
        content_el = await page.query_selector("#content")
        if not content_el:
            continue

        # For double elimination: the first table is the main bracket,
        # subsequent tables may be Play-off 3/4 and Consolation.
        # We want the Winner column from the FIRST (main) table only.
        winner_name = None
        winner_usab = None
        tables = await content_el.query_selector_all("table")
        found_first = False
        for table in tables:
            rows = await table.query_selector_all("tr")
            for row in rows:
                cells = await row.query_selector_all("td")
                if len(cells) < 7:
                    continue
                last_cell = cells[-1]
                link = await last_cell.query_selector("a[href*='player=']")
                if link:
                    name = (await link.inner_text()).strip()
                    name = re.sub(r"\s*\[\d+(?:/\d+)?\]", "", name).strip()
                    href = await link.get_attribute("href") or ""
                    pid_m = re.search(r"player=(\d+)", href)
                    if pid_m:
                        winner_name = name
                        winner_usab = ts_to_usab.get(pid_m.group(1))
                        found_first = True
            if found_first:
                break  # Stop after first table (main bracket)

        if winner_name:
            winners[d["name"]] = {"name": winner_name, "usab_id": winner_usab}
        print(f"  {d['name']}: {winner_name or 'N/A'}")

    return winners


def validate(matches, bracket_winners):
    """Validate all matches."""
    errors = []
    warnings = []

    for i, m in enumerate(matches):
        t1, t2 = m.get("team1", []), m.get("team2", [])
        scores = m.get("scores", [])
        winner = m.get("winner")
        is_wo = m.get("walkover", False)
        is_dbl = any(m["event"].startswith(x) for x in ("BD", "GD", "XD", "Doubles"))
        exp = 2 if is_dbl else 1

        if not is_wo:
            if len(t1) != exp:
                warnings.append(f"Match {i} ({m['event']} {m.get('round','')}): team1 size {len(t1)}, expected {exp}")
            if len(t2) != exp:
                warnings.append(f"Match {i} ({m['event']} {m.get('round','')}): team2 size {len(t2)}, expected {exp}")

        if scores and winner and not is_wo:
            t1w = sum(1 for s in scores if s[0] > s[1])
            t2w = sum(1 for s in scores if s[1] > s[0])
            sw = 1 if t1w > t2w else 2 if t2w > t1w else None
            if sw and sw != winner:
                errors.append(f"Match {i} ({m['event']} {m.get('round','')}): winner={winner} but scores say {sw}")

    # Check finals vs bracket
    for m in matches:
        if m.get("round") == "Final":
            event = m["event"]
            bk = None
            for k in bracket_winners:
                if k.startswith(event) and "Group" not in k:
                    bk = k
                    break
            if bk and bk in bracket_winners:
                bw = bracket_winners[bk]
                wt = m.get("team1") if m.get("winner") == 1 else m.get("team2") if m.get("winner") == 2 else []
                wids = {p.get("usab_id") for p in wt if p.get("usab_id")}
                bids = {bw.get("usab_id")} if isinstance(bw, dict) and bw.get("usab_id") else set()
                if bids and wids and not bids.issubset(wids):
                    errors.append(f"FINAL MISMATCH {event}: match winner IDs={wids}, bracket winner={bw}")

    return errors, warnings


async def main(tid):
    out = Path(f"data/tournament_results/{tid}")
    out.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Overview
        print("Phase 1: Overview...")
        info = await scrape_overview(page, tid)
        print(f"  {info['name']}, {info['dates']}")

        # Draws
        print("Phase 2: Draws...")
        draws = await scrape_draws(page, tid)
        (out / "draws.json").write_text(json.dumps(draws, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"  {len(draws)} draws")

        # Players
        print("Phase 3: Players...")
        players = await scrape_players(page, tid)
        (out / "players.json").write_text(json.dumps(players, indent=2, ensure_ascii=False), encoding="utf-8")
        with_id = sum(1 for p in players.values() if p.get("usab_id"))
        print(f"  {len(players)} players ({with_id} with USAB IDs)")

        ts_to_usab = {pid: p["usab_id"] for pid, p in players.items() if p.get("usab_id")}

        # Matches from each draw
        print("Phase 4: Matches...")
        all_matches = []
        for d in draws:
            name, matches = await scrape_draw_matches(page, tid, d["draw_id"], d["name"], ts_to_usab)
            all_matches.extend(matches)
            print(f"  {name}: {len(matches)} matches")

        (out / "matches.json").write_text(json.dumps(all_matches, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"  Total: {len(all_matches)} matches")

        # Bracket winners
        print("Phase 5: Bracket winners...")
        bracket_winners = await get_bracket_winners(page, tid, draws, ts_to_usab)
        (out / "bracket_winners.json").write_text(json.dumps(bracket_winners, indent=2, ensure_ascii=False), encoding="utf-8")

        await browser.close()

    # Validate
    print("\nPhase 6: Validation...")
    errors, warnings = validate(all_matches, bracket_winners)

    print(f"\n  ERRORS: {len(errors)}")
    for e in errors:
        print(f"    !! {e}")
    print(f"  WARNINGS: {len(warnings)}")
    if warnings:
        print(f"    (showing first 10)")
        for w in warnings[:10]:
            print(f"    ?? {w}")

    # Save tournament.json
    tournament = {
        "tournament_id": tid,
        "name": info["name"],
        "dates": info["dates"],
        "venue": info["venue"],
        "players": {p["usab_id"]: p for p in players.values() if p.get("usab_id")},
        "matches": all_matches,
        "draws": draws,
        "bracket_winners": bracket_winners,
    }
    (out / "tournament.json").write_text(json.dumps(tournament, indent=2, ensure_ascii=False), encoding="utf-8")

    report = {"errors": errors, "warnings": warnings}
    (out / "validation_report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\nDone! {len(players)} players, {len(all_matches)} matches, {len(errors)} errors")
    print(f"Saved to {out}/")


if __name__ == "__main__":
    tid = sys.argv[1] if len(sys.argv) > 1 else "4C4A21C2-E62B-4448-8B0D-12AEE6263B99"
    asyncio.run(main(tid))
