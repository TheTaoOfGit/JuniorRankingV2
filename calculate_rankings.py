"""
Calculate rankings from scraped tournament match data.
For a given discipline (e.g. GS U13) and cutoff date:
1. Find all tournaments in the last 12 months
2. For each player, determine their finish position at each tournament
3. Look up points from points table
4. Take top 4 results
5. Sum → rank
"""
import json
import re
import sys
import io
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
DATA = Path("data")


def load_points_table():
    return json.loads((DATA / "points_table.json").read_text(encoding="utf-8"))


def get_tournament_type(name, schedule_type=None):
    """Determine tournament type. Use schedule_type from tournaments.json if available."""
    if schedule_type:
        st = schedule_type.upper()
        if "NATIONAL" in st:
            return "JN"
        if st == "ORC":
            return "ORC"
        if st == "CRC":
            return "CRC"
        if st in ("OLC", "JDT"):
            return "OLC"
        if "SELECTION" in st:
            return "ORC"  # Selection events use ORC points
    # Fallback: guess from name
    name_upper = name.upper()
    if "NATIONAL" in name_upper:
        return "JN"
    if "OPEN REGIONAL" in name_upper:
        return "ORC"
    if "CLOSED REGIONAL" in name_upper:
        return "CRC"
    if "OPEN LOCAL" in name_upper:
        return "OLC"
    if "ORC" in name_upper:
        return "ORC"
    if "CRC" in name_upper:
        return "CRC"
    if "OLC" in name_upper:
        return "OLC"
    if "SELECTION" in name_upper:
        return "ORC"
    return "OLC"


def parse_tournament_date(dates_str, name):
    """Parse tournament date string to datetime."""
    # Format: "M/D/YY-M/D/YY" or "M/D/YY - M/D/YY"
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", dates_str or "")
    if m:
        yr = m.group(3)
        yr = "20" + yr if len(yr) == 2 else yr
        return datetime(int(yr), int(m.group(1)), int(m.group(2)))
    # Try extracting year from tournament name
    ym = re.search(r"(20\d{2})", name or "")
    if ym:
        return datetime(int(ym.group(1)), 6, 1)  # Approximate mid-year
    return None


def determine_finish_position(matches, usab_id, is_doubles=False, all_event_matches=None, tournament_type=None):
    """
    Determine a player's finish position from their match results.

    In elimination:
    - Won final = 1st
    - Lost final = 2nd
    - Lost semi = 3-4th (or 3rd/4th if playoff exists)
    - Lost QF = 5-8th
    - Lost R16 = 9-16th
    - Lost R32 = 17-32nd
    - etc.

    In consolation (double elimination):
    - Won consolation final = based on consolation bracket size
    - Lost in consolation = lower placement

    For round robin: use final standings position.
    """
    if not matches:
        return None

    def player_in_team(team, uid):
        return any(p.get("usab_id") == uid for p in (team or []))

    def player_won(m, uid):
        on_t1 = player_in_team(m.get("team1"), uid)
        return (on_t1 and m.get("winner") == 1) or (not on_t1 and m.get("winner") == 2)

    main_matches = [m for m in matches if m.get("bracket") != "consolation"]
    consolation_matches = [m for m in matches if m.get("bracket") == "consolation"]

    def round_to_size(rnd):
        """Map round name to bracket position size."""
        rnd_lower = (rnd or "").lower()
        if "final" in rnd_lower and "semi" not in rnd_lower and "quarter" not in rnd_lower and "consolation" not in rnd_lower:
            return 2
        if "semi" in rnd_lower:
            return 4
        if "quarter" in rnd_lower:
            return 8
        m = re.search(r"round of (\d+)", rnd_lower)
        if m:
            return int(m.group(1))
        return None

    # Check main bracket result
    won_main_final = False
    lost_main_final = False
    won_3rd = False
    lost_3rd = False
    deepest_main_loss_size = None

    for m in main_matches:
        rnd = m.get("round") or ""
        won = player_won(m, usab_id)

        if rnd == "Final":
            if won: won_main_final = True
            else: lost_main_final = True
        elif "3rd/4th" in rnd:
            if won: won_3rd = True
            else: lost_3rd = True
        elif not won:
            sz = round_to_size(rnd)
            if sz and (deepest_main_loss_size is None or sz < deepest_main_loss_size):
                deepest_main_loss_size = sz

    if won_main_final:
        return 1
    if lost_main_final:
        return 2
    if won_3rd:
        return 3
    if lost_3rd:
        return 4

    # SF losers without 3rd/4th match data: default to 4
    # (Conservative: without the actual 3rd/4th playoff result, assume 4th.
    # This matches official ranking behavior more closely.)
    if deepest_main_loss_size == 4:  # Lost in semi-final
        return 4

    # DOUBLE ELIMINATION: consolation only counts at JN for ranking positions.
    # At ORCs/OLCs/CRCs, consolation is just for extra games, not ranking.
    is_jn = tournament_type == "JN"
    if consolation_matches and is_jn:
        won_cons_final = False
        lost_cons_final = False
        won_cons_semi = False
        lost_cons_semi = False
        won_cons_qf = False
        lost_cons_qf = False
        deepest_cons_loss_size = None
        cons_wins = 0
        cons_losses = 0

        for m in consolation_matches:
            rnd = (m.get("round") or "").lower()
            won = player_won(m, usab_id)
            if won:
                cons_wins += 1
            else:
                cons_losses += 1

            if "final" in rnd and "semi" not in rnd and "quarter" not in rnd:
                if won: won_cons_final = True
                else: lost_cons_final = True
            elif "semi" in rnd:
                if won: won_cons_semi = True
                elif not won: lost_cons_semi = True
            elif "quarter" in rnd:
                if won: won_cons_qf = True
                else: lost_cons_qf = True
            elif not won:
                sz = round_to_size(rnd)
                if sz and (deepest_cons_loss_size is None or sz < deepest_cons_loss_size):
                    deepest_cons_loss_size = sz

        # Check if main bracket has a 3rd/4th playoff
        check_pool = all_event_matches if all_event_matches else matches
        has_3rd_4th_playoff = any("3rd/4th" in (m.get("round") or "") for m in check_pool if m.get("bracket") != "consolation")

        if has_3rd_4th_playoff:
            # JN-style: main bracket covers 1-4, consolation covers 5+
            # Use the best (deepest) round reached, handling mislabeled rounds
            # by checking both explicit round flags and total wins.
            if won_cons_final:
                return 5
            if lost_cons_final:
                return 6
            if won_cons_semi:
                return 5
            if lost_cons_semi:
                return 7  # 7-8
            if won_cons_qf:
                return 9  # Won cons QF -> at least 9th (will play cons SF)

            # Use deepest loss, but if multiple losses, some are mislabeled.
            if deepest_cons_loss_size:
                cons_pos_map = {
                    8: 9,     # Consolation QF loss -> 9-12
                    16: 13,   # Consolation R16 loss -> 13-16
                    32: 17,   # Consolation R32 loss -> 17-24
                    64: 25,   # Consolation R64 loss -> 25-32
                    128: 33,  # Consolation R128 loss -> 33-48
                    256: 49,  # Consolation R256 loss -> 49-64
                }
                pos_from_loss = cons_pos_map.get(deepest_cons_loss_size, deepest_cons_loss_size)

                # If multiple cons losses AND some wins, estimate from total matches
                if cons_losses > 1 and cons_wins > 0:
                    effective_cons_wins = cons_wins + cons_losses - 1
                    wins_pos_map = {0: 33, 1: 17, 2: 9, 3: 7, 4: 5}
                    pos_from_wins = wins_pos_map.get(effective_cons_wins, 5)
                    return min(pos_from_loss, pos_from_wins)

                return pos_from_loss

            if lost_cons_qf:
                return 9  # 9-12
        else:
            # Non-JN: consolation doesn't count for ranking
            pass

    # SINGLE ELIMINATION: use wins-based formula (more robust than round labels,
    # which can be unreliable from scraping).
    # Determine draw size from all event matches
    check = all_event_matches if all_event_matches else matches
    draw_size = 0
    for m in check:
        if m.get("bracket") == "consolation":
            continue
        sz = round_to_size(m.get("round"))
        if sz and sz > draw_size:
            draw_size = sz

    # If round names give a draw size, use it (it reflects the actual bracket structure).
    # Only fall back to player count if no round names are available.
    if draw_size == 0 and check:
        all_players = set()
        for m in check:
            if m.get("bracket") == "consolation":
                continue
            for team in [m.get("team1", []), m.get("team2", [])]:
                for p in team:
                    if p.get("usab_id"):
                        all_players.add(p["usab_id"])
        if all_players:
            # Round up to next power of 2
            n = len(all_players)
            draw_size = 1
            while draw_size < n:
                draw_size *= 2

    main_wins = sum(1 for m in main_matches if player_won(m, usab_id))
    main_losses = sum(1 for m in main_matches if not player_won(m, usab_id))

    # In single elimination, >1 main loss means some losses are actually consolation
    # matches mislabeled as main. When this happens, count total matches (wins+losses)
    # to infer how deep the player went: they must have won all but their final match
    # in the main bracket. Total main matches = wins + 1 (the elimination loss).
    # If player has only wins and no losses, their elimination match was not scraped;
    # use wins to estimate position (they advanced at least this far).
    if main_wins > 0 or main_losses > 0:
        # Prefer deepest loss round label (handles byes correctly).
        if deepest_main_loss_size:
            pos = deepest_main_loss_size // 2 + 1

            # If multiple losses, some are mislabeled consolation or duplicates.
            # The deepest loss round label is the most reliable indicator — just use it.

            return pos

        # No main loss recorded (elimination match not scraped).
        # Conservative estimate: assume player lost in the round after their last win.
        # With W wins, they advanced W rounds from the first round.
        # Position = draw_size / 2^W + 1
        if draw_size > 0:
            divisor = 2 ** main_wins
            pos = 2 if divisor >= draw_size else draw_size // divisor + 1
            return pos

        return None

    # Round robin: count wins to determine position
    rr_matches = [m for m in matches if m.get("round") and re.match(r"Round \d+$", m["round"])]
    if rr_matches:
        wins = sum(1 for m in rr_matches if player_won(m, usab_id))
        total = len(rr_matches)
        # Approximate: more wins = better position
        return max(1, total + 1 - wins)

    return None


def position_to_display(position, tournament_type="ORC"):
    """Convert numeric position to display string like '5-8', '9-12', etc."""
    if position is None:
        return "?"
    if position <= 4:
        return str(position)
    if tournament_type == "JN":
        # JN has individual positions for top 6, then ranges
        if position <= 6:
            return str(position)
        if position <= 8: return "7-8"
        if position <= 12: return "9-12"
        if position <= 16: return "13-16"
        if position <= 24: return "17-24"
        if position <= 32: return "25-32"
        if position <= 48: return "33-48"
        if position <= 64: return "49-64"
        if position <= 96: return "65-96"
        if position <= 128: return "97-128"
        return "129-256"
    else:
        # ORC/OLC/CRC use ranges from 5 onwards
        if position <= 8: return "5-8"
        if position <= 16: return "9-16"
        if position <= 32: return "17-32"
        if position <= 64: return "33-64"
        if position <= 128: return "65-128"
        return "129-256"


def position_to_points_key(position):
    """Convert numeric position to points table key."""
    if position == 1: return "1"
    if position == 2: return "2"
    if position == 3: return "3"
    if position == 4: return "4"
    if position == 5: return "5"
    if position == 6: return "6"
    if position in (7, 8): return "7-8"
    if 5 <= position <= 8: return "5-8"
    if 9 <= position <= 12: return "9-12"
    if 13 <= position <= 16: return "13-16"
    if 9 <= position <= 16: return "9-16"
    if 17 <= position <= 24: return "17-24"
    if 25 <= position <= 32: return "25-32"
    if 17 <= position <= 32: return "17-32"
    if 33 <= position <= 48: return "33-48"
    if 49 <= position <= 64: return "49-64"
    if 33 <= position <= 64: return "33-64"
    if 65 <= position <= 96: return "65-96"
    if 97 <= position <= 128: return "97-128"
    if 65 <= position <= 128: return "65-128"
    if 129 <= position <= 256: return "129-256"
    return None


def lookup_points(points_table, age_group, tournament_type, position):
    """Look up points for a given age group, tournament type, and position."""
    age_table = points_table.get(age_group, {})
    type_table = age_table.get(tournament_type, {})

    # Try exact key first, then range keys
    key = position_to_points_key(position)
    if key and key in type_table:
        return type_table[key]

    # For ORC/OLC, positions 5-8 are grouped. Try the grouped key.
    if 5 <= position <= 8:
        for k in ["5", "5-8"]:
            if k in type_table:
                return type_table[k]
    if 9 <= position <= 16:
        for k in ["9-12", "9-16"]:
            if k in type_table:
                return type_table[k]
    if 13 <= position <= 16:
        for k in ["13-16", "9-16"]:
            if k in type_table:
                return type_table[k]
    if 17 <= position <= 32:
        for k in ["17-24", "25-32", "17-32"]:
            if k in type_table:
                return type_table[k]
    if 33 <= position <= 64:
        for k in ["33-48", "49-64", "33-64"]:
            if k in type_table:
                return type_table[k]
    if 65 <= position <= 128:
        for k in ["65-96", "97-128", "65-128"]:
            if k in type_table:
                return type_table[k]
    if 129 <= position <= 256:
        if "129-256" in type_table:
            return type_table["129-256"]

    return 0


def calculate_rankings(discipline, age_group, cutoff_date_str, top_n=4):
    """Calculate rankings for a discipline/age group as of cutoff date."""
    cutoff = datetime.strptime(cutoff_date_str, "%Y-%m-%d")
    start = cutoff - timedelta(days=365)

    points_table = load_points_table()
    tournaments_index = json.loads((DATA / "tournaments.json").read_text(encoding="utf-8"))
    results_dir = DATA / "tournament_results"
    birth_years = json.loads((DATA / "inferred_birth_years.json").read_text(encoding="utf-8"))

    is_doubles = discipline.startswith(("BD", "GD", "XD"))

    # Age group eligibility: U13 = born 2014+, U11 = born 2016+, etc.
    # The cutoff year is the season year (Jan 1 of the year the season starts)
    # For 2025-2026 season, cutoff is Jan 1, 2026
    # U13 means under 13 on Jan 1 2026, so born 2014 or later (age 11-12)
    # U11 means under 11 on Jan 1 2026, so born 2016 or later (age 9-10)
    age_limit = int(age_group[1:])  # 13 for U13
    season_year = cutoff.year  # Use cutoff year as season year
    min_birth_year = season_year - age_limit + 1  # 2026 - 13 + 1 = 2014

    def is_age_eligible(usab_id):
        """Check if player is eligible for this age group (born min_birth_year or later)."""
        norm_id = usab_id.lstrip("0") or usab_id
        by = birth_years.get(norm_id, birth_years.get(usab_id, {}))
        yob = by.get("yob_inferred")
        if yob is None:
            return True  # If we don't know, include them
        return yob >= min_birth_year

    print(f"Age eligibility: {age_group} = born {min_birth_year} or later")

    # Find all tournaments in the date range
    eligible_tournaments = []
    for season, tlist in tournaments_index.items():
        for t in tlist:
            url = t.get("tournamentsoftware_url", "")
            m = re.search(r"[?/](?:id=)?([A-Fa-f0-9-]{36})", url or "")
            if not m:
                continue
            tid = m.group(1).upper()
            tourn_date = parse_tournament_date(t.get("dates", ""), t.get("name", ""))
            if not tourn_date:
                continue
            if start <= tourn_date < cutoff:
                tourn_type = get_tournament_type(t.get("name", ""), t.get("type", ""))
                tp = results_dir / tid / "tournament.json"
                if tp.exists():
                    eligible_tournaments.append({
                        "tid": tid,
                        "name": t["name"],
                        "type": tourn_type,
                        "date": tourn_date,
                        "path": tp,
                    })

    print(f"Eligible tournaments ({len(eligible_tournaments)}):")
    for et in sorted(eligible_tournaments, key=lambda x: x["date"]):
        print(f"  {et['date'].strftime('%Y-%m-%d')} [{et['type']}] {et['name'][:55]}")

    # For each tournament, find all players and their positions in this discipline
    # Cross-age-group: younger AG results count for higher AG rankings.
    # If a player enters BOTH younger and target AG at the same tournament,
    # only the TARGET AG result counts (not the younger one).
    age_nums = {"U11": 11, "U13": 13, "U15": 15, "U17": 17, "U19": 19}
    target_age = age_nums.get(age_group, 13)
    age_groups_to_check = [ag for ag, num in sorted(age_nums.items(), key=lambda x: x[1]) if num <= target_age]
    event_names_all = [f"{discipline} {ag}" for ag in age_groups_to_check]
    print(f"Checking events: {event_names_all}")

    player_results = defaultdict(list)  # usab_id -> [(tournament_name, type, position, points)]
    player_names = {}  # usab_id -> name (from match data, fallback for Unknown)

    for et in eligible_tournaments:
        t = json.loads(et["path"].read_text(encoding="utf-8"))

        # Collect player names from tournament player list
        for uid, pinfo in t.get("players", {}).items():
            norm_uid = uid.lstrip("0") or uid  # Normalize leading zeros
            if pinfo.get("name") and norm_uid not in player_names:
                player_names[norm_uid] = pinfo["name"]

        player_best_at_tournament = {}  # usab_id -> best (position, points, event)

        for event_name in event_names_all:
            matches = [m for m in t.get("matches", []) if m.get("event") == event_name]

            if not matches:
                continue

            # Group matches by player (normalize leading zeros in USAB IDs)
            player_matches = defaultdict(list)
            for m in matches:
                for p in (m.get("team1", []) + m.get("team2", [])):
                    uid = p.get("usab_id")
                    if uid:
                        norm_uid = uid.lstrip("0") or uid
                        player_matches[norm_uid].append(m)
                        if p.get("name") and norm_uid not in player_names:
                            player_names[norm_uid] = p["name"]

            for uid, pmatches in player_matches.items():
                if not is_age_eligible(uid):
                    continue
                position = determine_finish_position(pmatches, uid, is_doubles, all_event_matches=matches, tournament_type=et["type"])
                if position:
                    event_ag = event_name.split()[-1]  # "GS U11" -> "U11"
                    pts = lookup_points(points_table, event_ag, et["type"], position)
                    is_target = (event_ag == age_group)
                    prev = player_best_at_tournament.get(uid)

                    if prev is None:
                        player_best_at_tournament[uid] = (position, pts, event_name, is_target)
                    elif is_target and not prev[3]:
                        # Target AG result overrides younger AG result
                        player_best_at_tournament[uid] = (position, pts, event_name, is_target)
                    elif not is_target and prev[3]:
                        # Don't override target AG with younger AG
                        pass
                    elif pts > prev[1]:
                        # Same priority, take better points
                        player_best_at_tournament[uid] = (position, pts, event_name, is_target)

        # Add the best result per player for this tournament
        for uid, (position, pts, event_name, _) in player_best_at_tournament.items():
            player_results[uid].append({
                "tournament": et["name"],
                "type": et["type"],
                "date": et["date"].strftime("%Y-%m-%d"),
                "position": position,
                "position_display": position_to_display(position, et["type"]),
                "points": pts,
                "event": event_name,
            })

    # Calculate final rankings: top N results per player
    rankings = []
    for uid, results in player_results.items():
        # Sort by points descending, take top N
        results.sort(key=lambda x: -x["points"])
        top_results = results[:top_n]
        total_points = sum(r["points"] for r in top_results)
        rankings.append({
            "usab_id": uid,
            "total_points": total_points,
            "top_results": top_results,
            "all_results": results,
        })

    rankings.sort(key=lambda x: -x["total_points"])

    # Add names (prefer birth_years, fall back to tournament match data)
    for r in rankings:
        by = birth_years.get(r["usab_id"], {})
        name = by.get("name") or player_names.get(r["usab_id"]) or "Unknown"
        r["name"] = name
        r["yob"] = by.get("yob_inferred")

    return rankings


if __name__ == "__main__":
    disc = sys.argv[1] if len(sys.argv) > 1 else "GS"
    age = sys.argv[2] if len(sys.argv) > 2 else "U13"
    cutoff = sys.argv[3] if len(sys.argv) > 3 else "2026-03-01"

    print(f"Calculating {disc} {age} rankings as of {cutoff}\n")
    rankings = calculate_rankings(disc, age, cutoff)

    print(f"\n{'='*80}")
    print(f"  {disc} {age} Rankings (top 4 of last 12 months, cutoff {cutoff})")
    print(f"{'='*80}")

    # Compare with official
    official = json.loads(Path(f"data/rankings/{cutoff}_{disc}_{age}.json").read_text(encoding="utf-8"))
    official_map = {r["usab_id"]: (r["rank"], r["ranking_points"]) for r in official["rankings"]}

    print(f"\n{'Rank':>4} {'Name':<30} {'Born':>5} {'Calc Pts':>8} {'Official':>8} {'Match':>6}")
    print("-" * 85)
    for i, r in enumerate(rankings[:30], 1):
        off = official_map.get(r["usab_id"], (None, None))
        off_pts = off[1] if off[1] else ""
        match = "YES" if str(r["total_points"]) == str(off_pts) else "NO" if off_pts else "N/A"
        yob = r.get("yob", "?")
        print(f"{i:>4} {r['name']:<30} {yob:>5} {r['total_points']:>8} {off_pts:>8} {match:>6}")
        for tr in r["top_results"]:
            pd = tr.get('position_display', tr['position'])
            print(f"      pos={pd:>7} pts={tr['points']:>5} [{tr['type']}] {tr['tournament'][:45]}")
