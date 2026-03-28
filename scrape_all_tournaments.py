"""
Batch scrape all tournaments. Uses player-page approach for complete match data.
Resumable - skips tournaments that already have tournament.json.
All player data keyed by USAB ID.
Uses concurrent browser tabs for speed.
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
DATA = Path("data")
CONCURRENCY = 12  # parallel tabs


def get_tournament_list():
    tournaments = json.loads((DATA / "tournaments.json").read_text(encoding="utf-8"))
    seen = set()
    result = []
    for season, tlist in sorted(tournaments.items()):
        for t in tlist:
            url = t.get("tournamentsoftware_url")
            if not url:
                continue
            m = re.search(r"[?/](?:id=)?([A-Fa-f0-9-]{36})", url)
            if not m:
                continue
            tid = m.group(1)
            if tid.upper() in seen:
                continue
            seen.add(tid.upper())
            result.append({"tid": tid, "name": t["name"], "season": season, "type": t.get("type", ""), "url": url})
    return result


SKIP_WORDS = frozenset({"Products", "Planner", "Helpdesk", "LiveScore", "Ranking", "CENTER",
    "Social", "Privacy", "Disclaimer", "Cookies", "Download", "Branding",
    "Tournamentsoftware", "Tournament", "League", "Box", "Visual",
    "HELP", "helpdesk", "sales", "Our", "See", "Main Location",
    "Statistics", "Total", "Singles", "Doubles", "Home", "Tournaments",
    "Leagues", "Flex Ladders", "Clubs", "English", "Log In", "FAVORITE",
    "Overview", "Matches", "Draws", "Events", "Seeded entries", "More",
    "Win-Loss"})


def parse_player_page_text(text, html, name_to_usab, ts_to_usab):
    """Parse matches from a player page's text. Returns (usab_id, name, events, matches, updated_name_map)."""
    usab_id = None
    m = re.search(r"\((\d{5,7})\)", text)
    if m:
        usab_id = m.group(1)

    pname = None
    nm = re.search(r"\n([A-Z]{1,2})\n(.+?)(?:\n|$)", text)
    if nm:
        pname = re.sub(r"\s*\(\d+\)\s*$", "", nm.group(2).strip()).strip()

    events = list(dict.fromkeys(re.findall(r"((?:BS|GS|BD|GD|XD|Doubles|Shonai [BG]S) U\d+)", text)))

    # Update name map from links
    for lm in re.finditer(r'player\.aspx\?id=[^&]+&(?:amp;)?player=(\d+)"[^>]*>([^<]+)</a>', html):
        t_id = lm.group(1)
        raw = re.sub(r"\s*\[\d+(?:/\d+)?\]", "", lm.group(2).strip()).strip()
        uid = ts_to_usab.get(t_id)
        if uid and raw:
            name_to_usab[raw] = uid

    # Parse matches
    blocks = text.split("H2H")
    matches = []

    for block in blocks[:-1]:
        block = block.strip()
        if not block or len(block) < 10:
            continue

        lines = [l.strip() for l in block.split("\n") if l.strip()]
        event = round_name = time_str = None
        is_walkover = "Walkover" in block
        player_lines = []
        w_index = None
        score_nums = []
        found_context = False  # Only collect players after event/round line

        for line in lines:
            clean = re.sub(r"\s*\[\d+(?:/\d+)?\]", "", line).strip()
            if re.match(r"^(BS|GS|BD|GD|XD) U\d+$", line):
                event = line; found_context = True; continue
            if re.match(r"^(Round of \d+|Round \d+|Quarter final|Semi final|Consolation.*final|Consolation.*round|Consolation.*quarter|Consolation.*semi|Final|Play-off|3rd/4th place)", line, re.IGNORECASE):
                round_name = line; found_context = True; continue
            if re.match(r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun) \d+/\d+/\d+ \d+:\d+ [AP]M", line):
                time_str = line; continue
            if re.match(r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)", line):
                continue
            if clean in ("Walkover", "Bye", "", "Played", "Matches", "Retired"): continue
            if "Retired" in clean: is_walkover = True; continue
            if any(sw in clean for sw in SKIP_WORDS): continue
            if not found_context: continue  # Skip everything before event/round line
            if clean == "W": w_index = len(player_lines); continue
            if clean == "L": w_index = -(len(player_lines)); continue  # negative = loser marker
            if re.match(r"^\d{1,2}$", clean): score_nums.append(int(clean)); continue
            # Skip venue/court patterns
            if re.match(r"^.+ - [A-Z]?\d", clean): continue
            if re.match(r"^.+ - [A-Z]$", clean): continue
            if re.match(r"^(Capital|Main Location|NVBC|Bintang|Arena|DFW|Synergy|Bellevue|Frisco|Shannon Pohl|Saint|St\.|Harbour|Harbor|Volleyball Gym|Badminton Gym|Irving Convention|Podium|RiverCentre|Concord)", clean): continue
            if len(clean) > 2 and clean[0].isupper() and not re.match(r"^\d", clean):
                uid = name_to_usab.get(clean)
                player_lines.append({"name": clean, "usab_id": uid})

        if not event or not player_lines:
            continue

        game_scores = []
        si = 0
        while si < len(score_nums) - 1:
            s1, s2 = score_nums[si], score_nums[si + 1]
            if s1 <= 30 and s2 <= 30 and (s1 >= 11 or s2 >= 11):
                game_scores.append([s1, s2]); si += 2
            else:
                si += 1

        if not game_scores and not is_walkover:
            continue

        is_dbl = event.startswith(("BD", "GD", "XD"))
        ts = 2 if is_dbl else 1

        # Handle L marker: negative w_index means the player before L lost
        # Convert L marker to W marker for the other side
        if w_index is not None and w_index < 0:
            l_pos = -w_index  # position of the loser
            # In singles: L after player 1 means player 2 won (w_index should indicate player 2 wins)
            # In doubles: L after pair 1 (pos 2) means pair 2 won
            if is_dbl:
                w_index = 4 if l_pos <= 2 else 2  # flip: loser's pair -> other pair wins
            else:
                w_index = 2 if l_pos <= 1 else 1  # flip: loser -> other wins

        if w_index is not None and len(player_lines) >= ts * 2:
            if is_dbl:
                t1, t2 = player_lines[:2], player_lines[2:4]
                winner = 1 if w_index == 2 else 2 if w_index >= 4 else None
            else:
                t1, t2 = [player_lines[0]], [player_lines[1]]
                winner = 1 if w_index == 1 else 2 if w_index >= 2 else None
        elif len(player_lines) >= ts * 2:
            t1, t2 = player_lines[:ts], player_lines[ts:ts*2]
            tw1 = sum(1 for s in game_scores if s[0] > s[1])
            tw2 = sum(1 for s in game_scores if s[1] > s[0])
            winner = 1 if tw1 > tw2 else 2 if tw2 > tw1 else None
        else:
            t1, t2 = player_lines[:ts], player_lines[ts:]
            winner = 1 if is_walkover else None

        if is_walkover and winner is None:
            winner = 1

        # On player pages, scores are always listed winner-first.
        # Reorient so scores reflect [team1_score, team2_score].
        # If winner is team2, the raw scores are [team2_score, team1_score] → flip.
        if game_scores and winner == 2:
            game_scores = [[s[1], s[0]] for s in game_scores]

        # If no W/L marker was found, use scores to determine winner
        if winner is None and game_scores and not is_walkover:
            tw1 = sum(1 for s in game_scores if s[0] > s[1])
            tw2 = sum(1 for s in game_scores if s[1] > s[0])
            winner = 1 if tw1 > tw2 else 2 if tw2 > tw1 else None

        bracket = "consolation" if round_name and "consolation" in round_name.lower() else "main"
        matches.append({
            "event": event, "round": round_name, "bracket": bracket,
            "team1": t1, "team2": t2, "scores": game_scores,
            "winner": winner, "walkover": is_walkover, "time": time_str,
        })

    return usab_id, pname, events, matches


async def scrape_tournament(browser, tid, name):
    out = DATA / "tournament_results" / tid.upper()
    out.mkdir(parents=True, exist_ok=True)

    page = await browser.new_page()

    # Phase 1: Overview
    await page.goto(f"{BASE}/tournament/{tid}", wait_until="networkidle", timeout=60000)
    try:
        await page.click("button:has-text('Accept')", timeout=3000)
        await asyncio.sleep(0.5)
    except:
        pass
    await page.wait_for_selector("body", timeout=10000)
    text = await page.inner_text("body")

    info_name = name
    dm = re.search(r"((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d+ (?:to|\-) (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)? ?\d+)", text)
    dates = dm.group(1) if dm else ""
    vm = re.search(r"ROUTE\s*\n(.+?)(?:\n|$)", text)
    venue = vm.group(1).strip() if vm else ""

    # Phase 2: Draws
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
    (out / "draws.json").write_text(json.dumps(draws, indent=2, ensure_ascii=False), encoding="utf-8")

    # Phase 3: Collect player IDs from draw pages
    all_pids = set()
    for d in draws:
        await page.goto(f"{BASE}/sport/draw.aspx?id={tid}&draw={d['draw_id']}", wait_until="networkidle", timeout=30000)
        h = await page.content()
        all_pids.update(re.findall(r"player=(\d+)", h))
    await page.close()

    print(f"    {len(all_pids)} players, {len(draws)} draws")

    # Phase 4: Scrape player pages concurrently
    pid_list = sorted(all_pids, key=int)
    players = {}
    all_matches = []
    ts_to_usab = {}
    name_to_usab = {}

    # First pass: get USAB IDs (concurrent)
    sem = asyncio.Semaphore(CONCURRENCY)
    pages_pool = [await browser.new_page() for _ in range(CONCURRENCY)]

    async def scrape_one_player(pg, ts_pid):
        async with sem:
            url = f"{BASE}/sport/player.aspx?id={tid}&player={ts_pid}"
            await pg.goto(url, wait_until="networkidle", timeout=30000)
            try:
                await pg.click("button:has-text('Accept')", timeout=1000)
                await asyncio.sleep(0.3)
            except:
                pass
            text = await pg.inner_text("body")
            html = await pg.content()
            return ts_pid, text, html

    # Process in batches
    total = len(pid_list)
    for batch_start in range(0, total, CONCURRENCY):
        batch = pid_list[batch_start:batch_start + CONCURRENCY]
        tasks = []
        for i, pid in enumerate(batch):
            pg = pages_pool[i % CONCURRENCY]
            tasks.append(scrape_one_player(pg, pid))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, Exception):
                continue
            ts_pid, text, html = r
            usab_id, pname, events, matches = parse_player_page_text(text, html, name_to_usab, ts_to_usab)

            if usab_id:
                ts_to_usab[ts_pid] = usab_id
                if pname:
                    clean = re.sub(r"\s*\(\d+\)\s*$", "", pname).strip()
                    name_to_usab[clean] = usab_id

            players[ts_pid] = {"ts_player_id": ts_pid, "usab_id": usab_id, "name": pname, "events": events}
            all_matches.extend(matches)

        done_count = min(batch_start + CONCURRENCY, total)
        if done_count % 100 == 0 or done_count == total:
            print(f"      [{done_count}/{total}] players, {len(all_matches)} raw matches")

    for pg in pages_pool:
        await pg.close()

    (out / "players.json").write_text(json.dumps(players, indent=2, ensure_ascii=False), encoding="utf-8")

    # Fix USAB IDs in matches using name_to_usab
    for m in all_matches:
        for team in [m["team1"], m["team2"]]:
            for p in team:
                if not p.get("usab_id") and p.get("name"):
                    clean = re.sub(r"\s*\[\d+(?:/\d+)?\]", "", p["name"]).strip()
                    if clean in name_to_usab:
                        p["usab_id"] = name_to_usab[clean]

    # Deduplicate
    seen = set()
    deduped = []
    for m in all_matches:
        pids = tuple(sorted((p.get("usab_id") or p.get("name", "?")) for p in m["team1"] + m["team2"]))
        skey = tuple(tuple(s) for s in m["scores"])
        key = (m["event"], m.get("round") or "", pids, skey, m.get("walkover", False))
        if key not in seen:
            seen.add(key)
            deduped.append(m)

    (out / "matches.json").write_text(json.dumps(deduped, indent=2, ensure_ascii=False), encoding="utf-8")

    # Phase 5: Bracket winners
    page = await browser.new_page()
    bracket_winners = {}
    for d in draws:
        if "Group" in d["name"]:
            continue
        await page.goto(f"{BASE}/sport/draw.aspx?id={tid}&draw={d['draw_id']}", wait_until="networkidle", timeout=30000)
        content_el = await page.query_selector("#content")
        if not content_el:
            continue
        tables = await content_el.query_selector_all("table")
        found = False
        for table in tables:
            rows = await table.query_selector_all("tr")
            for row in rows:
                cells = await row.query_selector_all("td")
                if len(cells) < 7:
                    continue
                last_cell = cells[-1]
                link = await last_cell.query_selector("a[href*='player=']")
                if link:
                    wname = re.sub(r"\s*\[\d+(?:/\d+)?\]", "", (await link.inner_text()).strip()).strip()
                    href = await link.get_attribute("href") or ""
                    pm = re.search(r"player=(\d+)", href)
                    if pm:
                        bracket_winners[d["name"]] = {"name": wname, "usab_id": ts_to_usab.get(pm.group(1))}
                        found = True
            if found:
                break
    await page.close()

    (out / "bracket_winners.json").write_text(json.dumps(bracket_winners, indent=2, ensure_ascii=False), encoding="utf-8")

    # Save tournament.json
    tournament = {
        "tournament_id": tid.upper(),
        "name": info_name,
        "dates": dates,
        "venue": venue,
        "players": {p["usab_id"]: {"usab_id": p["usab_id"], "name": re.sub(r"\s*\(\d+\)\s*$", "", p.get("name") or "").strip(), "ts_player_id": pid, "events": p.get("events", [])} for pid, p in players.items() if p.get("usab_id")},
        "matches": deduped,
        "draws": draws,
        "bracket_winners": bracket_winners,
    }
    (out / "tournament.json").write_text(json.dumps(tournament, indent=2, ensure_ascii=False), encoding="utf-8")

    # Validate
    score_errors = 0
    for m in deduped:
        if m.get("scores") and m.get("winner") and not m.get("walkover"):
            tw1 = sum(1 for s in m["scores"] if s[0] > s[1])
            tw2 = sum(1 for s in m["scores"] if s[1] > s[0])
            sw = 1 if tw1 > tw2 else 2 if tw2 > tw1 else None
            if sw and sw != m["winner"]:
                score_errors += 1

    with_id = sum(1 for p in players.values() if p.get("usab_id"))
    return len(players), with_id, len(deduped), len(bracket_winners), score_errors


async def main():
    all_tournaments = get_tournament_list()
    done_dir = DATA / "tournament_results"

    to_scrape = []
    for t in all_tournaments:
        out = done_dir / t["tid"].upper()
        if (out / "tournament.json").exists():
            continue
        # Clear stale error files
        if (out / "error.txt").exists():
            (out / "error.txt").unlink()
        to_scrape.append(t)

    print(f"Total: {len(all_tournaments)}, Done: {len(all_tournaments)-len(to_scrape)}, To scrape: {len(to_scrape)}")

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

        for i, t in enumerate(to_scrape, 1):
            print(f"\n[{i}/{len(to_scrape)}] {t['name']} ({t['tid'][:8]}...)")
            try:
                players, with_id, matches, winners, errors = await scrape_tournament(browser, t["tid"], t["name"])
                print(f"    -> {players} players ({with_id} ID'd), {matches} matches, {winners} winners, {errors} score errors")
            except Exception as e:
                print(f"    !! ERROR: {e}")
                out = done_dir / t["tid"].upper()
                out.mkdir(parents=True, exist_ok=True)
                (out / "error.txt").write_text(str(e), encoding="utf-8")

        await browser.close()

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
