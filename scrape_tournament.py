"""
Scrape a full tournament from tournamentsoftware.com
Captures: players (with USAB IDs), draws, matches, scores
"""

import asyncio
import json
import re
import sys
from pathlib import Path
from playwright.async_api import async_playwright

BASE = "https://www.tournamentsoftware.com"


async def accept_cookies(page):
    try:
        await page.click("button:has-text('Accept')", timeout=3000)
        await asyncio.sleep(0.5)
    except:
        pass


async def scrape_players(page, tid):
    """Scrape all players with their USAB member IDs from player pages."""
    players = {}  # ts_player_id -> {usab_id, name, club}

    # Visit each player page linked from the players list
    # First get all player links from A-Z
    await page.goto(f"{BASE}/sport/players.aspx?id={tid}", wait_until="networkidle", timeout=30000)
    await accept_cookies(page)

    all_player_links = set()
    # Click through each letter
    letters = await page.query_selector_all("a.nav-link")
    letter_hrefs = []
    for l in letters:
        txt = (await l.inner_text()).strip()
        href = await l.get_attribute("href")
        if href and len(txt) == 1 and txt.isalpha():
            letter_hrefs.append(href)

    # Also get players from current page
    async def extract_player_links(pg):
        links = await pg.query_selector_all("a")
        for link in links:
            href = await link.get_attribute("href") or ""
            if "player=" in href and "player.aspx" in href:
                m = re.search(r'player=(\d+)', href)
                if m:
                    all_player_links.add(m.group(1))

    await extract_player_links(page)

    for href in letter_hrefs:
        url = href if href.startswith("http") else f"{BASE}{href}"
        await page.goto(url, wait_until="networkidle", timeout=30000)
        await extract_player_links(page)

    print(f"  Found {len(all_player_links)} player links")

    # Now visit each player page to get USAB ID
    for pid in sorted(all_player_links):
        await page.goto(f"{BASE}/sport/player.aspx?id={tid}&player={pid}", wait_until="networkidle", timeout=30000)
        body = await page.query_selector("body")
        text = await body.inner_text()

        # Parse: "Name (USAB_ID)"
        m = re.search(r'([A-Z][^\n]*?)\s*\((\d+)\)', text)
        if m:
            name = m.group(1).strip()
            usab_id = m.group(2)
        else:
            # Try to get name without ID
            name = None
            usab_id = None

        # Get club - look for it near the name
        club = None
        # The player page shows club in the entries section
        # Try to find it from the H2H/match entries
        club_el = await page.query_selector(".player-club, .media-body small")
        if club_el:
            club = (await club_el.inner_text()).strip()

        players[pid] = {
            "ts_player_id": pid,
            "usab_id": usab_id,
            "name": name,
            "club": club,
        }

    return players


async def scrape_players_fast(page, tid):
    """Fast player scraping - get USAB IDs from draw pages where they appear as 'Name (ID)'."""
    players = {}  # ts_player_id -> {usab_id, name, club}

    # Get all player links and their names from the players list
    await page.goto(f"{BASE}/sport/players.aspx?id={tid}", wait_until="networkidle", timeout=30000)
    await accept_cookies(page)

    # Collect from all letter pages
    player_links = {}  # ts_player_id -> name from list

    async def collect_from_page(pg):
        links = await pg.query_selector_all("a")
        for link in links:
            href = await link.get_attribute("href") or ""
            if "player=" in href and "player.aspx" in href:
                m = re.search(r'player=(\d+)', href)
                if m:
                    txt = (await link.inner_text()).strip()
                    if txt and not txt.startswith("Player"):
                        player_links[m.group(1)] = txt

    await collect_from_page(page)

    # Get letter navigation links
    nav_links = await page.query_selector_all("a")
    letter_urls = []
    for l in nav_links:
        txt = (await l.inner_text()).strip()
        href = await l.get_attribute("href") or ""
        if len(txt) == 1 and txt.isalpha() and "players" in href:
            full = href if href.startswith("http") else f"{BASE}/sport/{href}" if not href.startswith("/") else f"{BASE}{href}"
            letter_urls.append(full)

    for url in letter_urls:
        await page.goto(url, wait_until="networkidle", timeout=30000)
        await collect_from_page(page)

    print(f"  Found {len(player_links)} players from list")

    # Now get USAB IDs from individual player pages (in batches)
    total = len(player_links)
    for i, (pid, name) in enumerate(sorted(player_links.items()), 1):
        await page.goto(f"{BASE}/sport/player.aspx?id={tid}&player={pid}", wait_until="networkidle", timeout=30000)
        body = await page.query_selector("body")
        text = await body.inner_text()

        usab_id = None
        m = re.search(r'\((\d{5,7})\)', text)
        if m:
            usab_id = m.group(1)

        # Get events this player is in
        events = []
        event_matches = re.findall(r'((?:BS|GS|BD|GD|XD|Doubles|Shonai [BG]S) U\d+)', text)
        events = list(dict.fromkeys(event_matches))  # dedupe preserving order

        players[pid] = {
            "ts_player_id": pid,
            "usab_id": usab_id,
            "name": name,
            "events": events,
        }

        if i % 50 == 0 or i == total:
            print(f"    [{i}/{total}] players scraped")

    return players


async def scrape_draws(page, tid):
    """Scrape the draws list to get all draw IDs and their event names."""
    await page.goto(f"{BASE}/sport/draws.aspx?id={tid}", wait_until="networkidle", timeout=30000)
    await accept_cookies(page)

    draws = []
    links = await page.query_selector_all("a")
    for link in links:
        href = await link.get_attribute("href") or ""
        txt = (await link.inner_text()).strip()
        if "draw=" in href and txt:
            m = re.search(r'draw=(\d+)', href)
            if m:
                draws.append({"draw_id": m.group(1), "name": txt})

    return draws


async def scrape_draw_detail(page, tid, draw_id, draw_name):
    """Scrape a single draw page for bracket/group data."""
    url = f"{BASE}/sport/draw.aspx?id={tid}&draw={draw_id}"
    await page.goto(url, wait_until="networkidle", timeout=30000)

    body = await page.query_selector("body")
    text = await body.inner_text()

    # Determine format
    is_round_robin = "Round Robin" in text
    is_elimination = "Elimination" in text

    # Get size
    size_match = re.search(r'Size (\d+)', text)
    size = int(size_match.group(1)) if size_match else None

    # Extract all player references with their ts IDs
    player_refs = []
    links = await page.query_selector_all("a")
    for link in links:
        href = await link.get_attribute("href") or ""
        if "player=" in href:
            m = re.search(r'player=(\d+)', href)
            if m:
                txt = (await link.inner_text()).strip()
                if txt and "Player" not in txt:
                    player_refs.append({"ts_player_id": m.group(1), "name": txt})

    # Extract matches from the Matches section
    matches = []
    # Find match rows - they contain score patterns like "21 15" or "Walkover"
    match_sections = re.split(r'(?:Round of \d+|Round \d+|Quarter final|Semi final|Final|Consolation)', text)

    # Better approach: parse the matches section at the bottom of the page
    matches_idx = text.rfind("Matches\n")
    if matches_idx >= 0:
        matches_text = text[matches_idx:]
        # Each match block has: round info, player names, scores, date
        # Parse match blocks using date pattern as delimiter
        match_blocks = re.split(r'H2H\n', matches_text)
        for block in match_blocks:
            block = block.strip()
            if not block:
                continue

            # Try to extract: event, round, players, scores, date
            lines = block.split('\n')
            match_data = {"raw": block[:200]}
            matches.append(match_data)

    draw_data = {
        "draw_id": draw_id,
        "name": draw_name,
        "format": "round_robin" if is_round_robin else "elimination" if is_elimination else "unknown",
        "size": size,
        "player_refs": player_refs[:50],  # Sample
        "num_matches": len(matches),
        "raw_text": text[text.find(draw_name):] if draw_name in text else text[:500],
    }

    return draw_data


async def scrape_matches_by_day(page, tid, day_url):
    """Scrape all matches for a given day."""
    await page.goto(day_url, wait_until="networkidle", timeout=30000)

    body = await page.query_selector("body")
    text = await body.inner_text()

    # Get all player links in match context
    player_id_map = {}
    links = await page.query_selector_all("a")
    for link in links:
        href = await link.get_attribute("href") or ""
        if "player=" in href:
            m = re.search(r'player=(\d+)', href)
            if m:
                txt = (await link.inner_text()).strip()
                # Remove seed notation like [1], [3/4]
                clean = re.sub(r'\s*\[\d+(?:/\d+)?\]', '', txt).strip()
                if clean:
                    player_id_map[clean] = m.group(1)

    # Parse matches from the HTML table structure
    # Each match card has: event, round, player1(+partner), player2(+partner), scores, time, venue
    matches = []

    # Get match containers
    match_els = await page.query_selector_all(".match-group .match, [class*='match']")

    # If that doesn't work, parse from text
    # Split by time slots
    return {"player_id_map": player_id_map, "raw_text_length": len(text), "text": text}


async def scrape_full_tournament(tid):
    """Main function to scrape an entire tournament."""
    out_dir = Path("data/tournament_results") / tid
    out_dir.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context()
        page = await ctx.new_page()

        # 1. Scrape players
        print("Phase 1: Scraping players...")
        players = await scrape_players_fast(page, tid)
        (out_dir / "players.json").write_text(
            json.dumps(players, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"  Saved {len(players)} players")

        # Build usab_id lookup
        usab_lookup = {}
        for pid, pdata in players.items():
            if pdata.get("usab_id"):
                usab_lookup[pid] = pdata["usab_id"]

        # 2. Scrape draws list
        print("Phase 2: Scraping draws...")
        draws = await scrape_draws(page, tid)
        (out_dir / "draws.json").write_text(
            json.dumps(draws, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"  Found {len(draws)} draws")

        # 3. Scrape each draw detail
        print("Phase 3: Scraping draw details...")
        draw_details = []
        for d in draws:
            print(f"  Scraping draw: {d['name']}...")
            detail = await scrape_draw_detail(page, tid, d["draw_id"], d["name"])
            draw_details.append(detail)
        (out_dir / "draw_details.json").write_text(
            json.dumps(draw_details, indent=2, ensure_ascii=False), encoding="utf-8")

        # 4. Scrape matches by day
        print("Phase 4: Scraping matches...")
        # Get the day links from the matches page
        await page.goto(f"{BASE}/tournament/{tid}/Matches", wait_until="networkidle", timeout=30000)
        await accept_cookies(page)

        day_links = []
        links = await page.query_selector_all("a")
        for link in links:
            href = await link.get_attribute("href") or ""
            txt = (await link.inner_text()).strip()
            if "/Matches" in href and ("MAR" in txt or "FRI" in txt or "SAT" in txt or "SUN" in txt or "2026" in href):
                full = href if href.startswith("http") else f"{BASE}{href}"
                day_links.append((txt, full))

        # Also get the day links from the nav tabs
        day_tabs = await page.query_selector_all("[class*='day'], [class*='tab'], a[href*='Matches']")
        for tab in day_tabs:
            href = await tab.get_attribute("href") or ""
            txt = (await tab.inner_text()).strip()
            if "Matches" in href and len(txt) < 20:
                full = href if href.startswith("http") else f"{BASE}{href}"
                if full not in [d[1] for d in day_links]:
                    day_links.append((txt, full))

        # Get match URLs for each day
        match_day_urls = [
            f"{BASE}/tournament/{tid}/Matches?Day=2026-03-13",
            f"{BASE}/tournament/{tid}/Matches?Day=2026-03-14",
            f"{BASE}/tournament/{tid}/Matches?Day=2026-03-15",
        ]

        all_matches_text = {}
        all_player_ids = {}
        for day_url in match_day_urls:
            day = day_url.split("Day=")[1] if "Day=" in day_url else "unknown"
            print(f"  Scraping matches for {day}...")
            result = await scrape_matches_by_day(page, tid, day_url)
            all_player_ids.update(result["player_id_map"])
            all_matches_text[day] = result["text"]

        # Save raw match text for parsing
        (out_dir / "matches_raw.json").write_text(
            json.dumps(all_matches_text, indent=2, ensure_ascii=False), encoding="utf-8")

        # Save player ID mapping (name -> ts_player_id from match pages)
        (out_dir / "player_id_map.json").write_text(
            json.dumps(all_player_ids, indent=2, ensure_ascii=False), encoding="utf-8")

        # 5. Now parse the match text into structured data
        print("Phase 5: Parsing matches...")
        parsed_matches = parse_all_matches(all_matches_text, all_player_ids, usab_lookup)
        (out_dir / "matches.json").write_text(
            json.dumps(parsed_matches, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"  Parsed {len(parsed_matches)} matches")

        await browser.close()

    print(f"\nDone! Data saved to {out_dir}/")
    return out_dir


def parse_all_matches(day_texts, player_id_map, usab_lookup):
    """Parse raw match text into structured match objects."""
    matches = []

    # Reverse lookup: name -> usab_id
    name_to_usab = {}
    for name, ts_id in player_id_map.items():
        usab_id = usab_lookup.get(ts_id)
        if usab_id:
            name_to_usab[name] = usab_id

    for day, text in sorted(day_texts.items()):
        # Split into match blocks - each ends with "H2H"
        blocks = text.split("H2H")

        for block in blocks:
            block = block.strip()
            if not block:
                continue

            lines = [l.strip() for l in block.split('\n') if l.strip()]

            # Try to find event and round
            event = None
            round_name = None
            for line in lines:
                if re.match(r'^(BS|GS|BD|GD|XD|Doubles|Shonai [BG]S) U\d+', line):
                    event = line
                elif re.match(r'^(Round of \d+|Round \d+|Quarter final|Semi final|Final|Play-off|Consolation)', line):
                    round_name = line

            # Find scores - patterns like "21 15" or "Walkover"
            scores = []
            score_pattern = re.findall(r'\b(\d{1,2})\s+(\d{1,2})\b', block)
            is_walkover = "Walkover" in block

            # Find W marker for winner
            has_winner = "\nW\n" in block or "\tW\t" in block

            # Extract player names (lines that look like names)
            player_lines = []
            for line in lines:
                # Skip known non-player lines
                if re.match(r'^(BS|GS|BD|GD|XD|Doubles|Shonai|Round|Quarter|Semi|Final|Play|Consolation|H2H|\d|W$|Walkover|Bye)', line):
                    continue
                if re.match(r'^\d{1,2}:\d{2}', line):  # time
                    continue
                if re.match(r'^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)', line):  # day
                    continue
                if re.match(r'^Bintang', line):  # venue
                    continue
                # Looks like a player name
                clean = re.sub(r'\s*\[\d+(?:/\d+)?\]', '', line).strip()
                if clean and len(clean) > 2:
                    player_lines.append(clean)

            if event and (score_pattern or is_walkover):
                # Map player names to USAB IDs
                mapped_players = []
                for pname in player_lines:
                    usab_id = name_to_usab.get(pname)
                    ts_id = player_id_map.get(pname)
                    mapped_players.append({
                        "name": pname,
                        "usab_id": usab_id,
                        "ts_player_id": ts_id,
                    })

                game_scores = []
                for s1, s2 in score_pattern:
                    game_scores.append([int(s1), int(s2)])

                match = {
                    "day": day,
                    "event": event,
                    "round": round_name,
                    "players": mapped_players,
                    "scores": game_scores,
                    "walkover": is_walkover,
                }
                matches.append(match)

    return matches


if __name__ == "__main__":
    tid = sys.argv[1] if len(sys.argv) > 1 else "5779DD58-5F08-4D64-A092-B41478B07A0A"
    asyncio.run(scrape_full_tournament(tid))
