"""Parse matches from locally saved draw pages. No network needed.
Uses the saved text files in data/tournament_pages/{tid}/draws/draw_{id}.txt
and player pages for USAB ID mapping.
"""
import json
import os
import re
import sys
from pathlib import Path
from collections import defaultdict

DATA = Path("data")
PAGES_DIR = DATA / "tournament_pages"


def parse_draw_text(text, html, draw_name, ts_to_usab):
    """Parse matches from a draw page's saved text and HTML."""

    # Build name -> usab_id from player links in HTML
    name_to_usab = {}
    for m in re.finditer(
        r'player\.aspx\?id=[^&]+&(?:amp;)?player=(\d+)"[^>]*>([^<]+)</a>', html
    ):
        t_id = m.group(1)
        raw_name = re.sub(r"\s*\[\d+(?:/\d+)?\]", "", m.group(2).strip()).strip()
        uid = ts_to_usab.get(t_id)
        if uid and raw_name:
            name_to_usab[raw_name] = uid

    is_doubles = any(draw_name.startswith(x) for x in ("BD", "GD", "XD", "Doubles"))

    # Find matches section
    matches_idx = text.find("Matches\nPlayed")
    if matches_idx < 0:
        matches_idx = text.rfind("Matches\n")
    if matches_idx < 0:
        return []

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
        is_walkover = "Walkover" in block or "Retired" in block
        player_lines = []
        w_index = None
        score_nums = []

        for line in lines:
            clean = re.sub(r"\s*\[\d+(?:/\d+)?\]", "", line).strip()
            rm = re.match(
                r"^(Round of \d+|Round \d+|Quarter final|Semi final|Final|Play-off|Consolation.*|3rd/4th place)",
                line, re.IGNORECASE,
            )
            if rm:
                round_name = rm.group(1)
                continue
            if re.match(r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun) \d+/\d+/\d+ \d+:\d+ [AP]M", line):
                time_str = line
                continue
            if re.match(r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)", line):
                continue
            if clean in ("Walkover", "Bye", "", "Played", "Matches", "Retired"):
                continue
            if clean == "W":
                w_index = len(player_lines)
                continue
            if clean == "L":
                w_index = -(len(player_lines))
                continue
            if "Retired" in clean:
                is_walkover = True
                continue
            if re.match(r"^\d{1,2}$", clean):
                score_nums.append(int(clean))
                continue
            # Skip non-player lines
            if any(kw in clean for kw in [
                "Products", "Planner", "Helpdesk", "LiveScore", "Ranking", "CENTER",
                "Social", "Privacy", "Disclaimer", "Cookies", "Download", "Club", "State",
                "Finals", "Winner",
            ]):
                continue
            if re.match(r"^.+ - [A-Z]?\d", clean) or re.match(r"^.+ - [A-Z]$", clean):
                continue
            # Skip venue/court patterns
            if re.match(r"^(Capital|Main Location|NVBC|Bintang|Arena|DFW|Synergy|Bellevue|Frisco|Shannon Pohl|Saint|St\.|Harbour|Harbor|Volleyball Gym|Badminton Gym|Irving Convention|Podium|RiverCentre|Concord)", clean):
                continue
            if len(clean) > 2 and clean[0].isupper() and not re.match(r"^\d", clean):
                uid = name_to_usab.get(clean)
                player_lines.append({"name": clean, "usab_id": uid})

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

        ts = 2 if is_doubles else 1

        # Handle L marker
        if w_index is not None and w_index < 0:
            l_pos = -w_index
            if is_doubles:
                w_index = 4 if l_pos <= 2 else 2
            else:
                w_index = 2 if l_pos <= 1 else 1

        if w_index is not None and len(player_lines) >= ts * 2:
            if is_doubles:
                t1, t2 = player_lines[:2], player_lines[2:4]
                winner = 1 if w_index == 2 else 2 if w_index >= 4 else None
            else:
                t1, t2 = [player_lines[0]], [player_lines[1]]
                winner = 1 if w_index == 1 else 2 if w_index >= 2 else None
        elif len(player_lines) >= ts * 2:
            # No W marker: on draw pages, 2nd player listed = winner (bracket convention)
            t1, t2 = player_lines[:ts], player_lines[ts:ts * 2]
            winner = 2
        else:
            t1, t2 = player_lines[:ts], player_lines[ts:]
            winner = 1 if is_walkover else None

        if is_walkover and winner is None:
            winner = 1

        # On draw pages, scores are [winner_score, loser_score].
        # Reorient to [team1_score, team2_score].
        if game_scores and winner == 2:
            game_scores = [[s[1], s[0]] for s in game_scores]

        event = draw_name.split(" - ")[0].strip()
        bracket = "consolation" if round_name and "consolation" in round_name.lower() else "main"
        matches.append({
            "event": event, "round": round_name, "bracket": bracket,
            "team1": t1, "team2": t2, "scores": game_scores,
            "winner": winner, "walkover": is_walkover, "time": time_str,
        })

    return matches


def parse_tournament(tid):
    """Parse all draw pages for a tournament from local files."""
    tid_dir = PAGES_DIR / tid
    results_dir = DATA / "tournament_results" / tid

    if not tid_dir.exists() or not results_dir.exists():
        return None

    players_path = results_dir / "players.json"
    draws_path = results_dir / "draws.json"
    tourn_path = results_dir / "tournament.json"

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

    # Parse each draw page
    draw_matches = []
    draws_dir = tid_dir / "draws"
    if not draws_dir.exists():
        return None

    for d in draws:
        draw_id = d["draw_id"]
        draw_name = d["name"]
        text_path = draws_dir / f"draw_{draw_id}.txt"
        html_path = draws_dir / f"draw_{draw_id}.html"

        if not text_path.exists() or not html_path.exists():
            continue

        text = text_path.read_text(encoding="utf-8")
        html = html_path.read_text(encoding="utf-8")
        matches = parse_draw_text(text, html, draw_name, ts_to_usab)
        draw_matches.extend(matches)

    if not draw_matches:
        return None

    # Deduplicate draw matches
    seen = set()
    draw_deduped = []
    for m in draw_matches:
        pids = tuple(sorted((p.get("usab_id") or p.get("name", "?")) for p in m["team1"] + m["team2"]))
        skey = tuple(tuple(s) for s in m["scores"])
        key = (m.get("event", ""), m.get("round") or "", pids, skey, m.get("walkover", False))
        if key not in seen:
            seen.add(key)
            draw_deduped.append(m)

    # Keep player-page matches not in draw data (supplemental)
    old_matches = tournament.get("matches", [])
    draw_player_sets = set()
    for m in draw_deduped:
        pids = frozenset((p.get("usab_id") or p.get("name", "?")) for p in m["team1"] + m["team2"])
        draw_player_sets.add((m.get("event", ""), pids))

    supplemental = []
    for m in old_matches:
        pids = frozenset((p.get("usab_id") or p.get("name", "?")) for p in m.get("team1", []) + m.get("team2", []))
        if (m.get("event", ""), pids) not in draw_player_sets:
            supplemental.append(m)

    final_matches = draw_deduped + supplemental

    # Update tournament.json and matches.json
    tournament["matches"] = final_matches
    tourn_path.write_text(json.dumps(tournament, ensure_ascii=False), encoding="utf-8")
    (results_dir / "matches.json").write_text(json.dumps(final_matches, ensure_ascii=False), encoding="utf-8")

    # Update tournaments_combined
    combined_path = DATA / "tournaments_combined" / f"{tid}.json"
    if combined_path.exists():
        combined_path.write_text(json.dumps(tournament, ensure_ascii=False), encoding="utf-8")

    return len(draw_deduped), len(supplemental)


def main():
    tids = sorted(os.listdir(PAGES_DIR))
    print(f"Parsing local draw pages for {len(tids)} tournaments")

    done = 0
    for i, tid in enumerate(tids, 1):
        if not (PAGES_DIR / tid / "draws").exists():
            continue
        try:
            result = parse_tournament(tid)
            done += 1
            if result:
                draw_count, supp_count = result
                # Get tournament name
                tp = DATA / "tournament_results" / tid / "tournament.json"
                name = json.loads(tp.read_text(encoding="utf-8")).get("name", tid)[:55] if tp.exists() else tid[:8]
                print(f"[{done}] {name}: {draw_count} draw + {supp_count} supp", flush=True)
        except Exception as e:
            print(f"[{done}] {tid[:8]}: ERROR {str(e)[:60]}", flush=True)

    print(f"\nDone! Parsed {done} tournaments.")


if __name__ == "__main__":
    main()
