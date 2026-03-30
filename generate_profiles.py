"""Generate player profile summaries and roasts using Claude CLI."""
import json
import os
import sys
import subprocess
import hashlib
from pathlib import Path
from collections import defaultdict

DATA = Path("data")
SUMMARIES_DIR = DATA / "player_summaries"
PLAYER_DATA_DIR = DATA / "player_data_for_summary"
SUMMARIES_DIR.mkdir(exist_ok=True)
PLAYER_DATA_DIR.mkdir(exist_ok=True)

ROAST_STYLES = [
    "David Attenborough nature documentary narration",
    "ESPN 30-for-30 documentary voiceover",
    "fake college admissions review committee notes",
    "therapist session notes about the player",
    "breakup letter from badminton to the player",
    "detective noir monologue investigating the player's career",
    "mock Wikipedia article with [citation needed] tags",
    "a Yelp review of the player as if they were a restaurant",
    "a real estate listing selling the player's career",
    "a weather forecast using the player's results as meteorological events",
    "a nature trail guide describing the player's career as a hiking trail",
    "a film critic reviewing the player's career as a movie",
    "a sommelier describing the player's career as a wine tasting",
    "a tech startup pitch deck for the player",
    "an archaeological dig report uncovering the player's career",
    "a coach's halftime speech about the player written as a dramatic monologue",
]


def get_roast_style(name):
    h = int(hashlib.md5(name.encode()).hexdigest(), 16)
    return ROAST_STYLES[h % len(ROAST_STYLES)]


def build_player_data():
    """Build player data files from tournament and ranking data."""
    birth_years = json.loads((DATA / "inferred_birth_years.json").read_text(encoding="utf-8"))
    rankings = json.loads((DATA / "calculated_rankings_monthly/2026-04-01.json").read_text(encoding="utf-8"))

    # Build per-player ranking info
    player_rankings = defaultdict(list)
    for key, players in rankings.items():
        if key == "alumni":
            continue
        for p in players:
            player_rankings[p["usab_id"]].append(f"{key.replace('_', ' ')} #{p['rank']} ({p['total_points']}pts)")

    # Build tournament history from tournaments_combined
    player_tournaments = defaultdict(list)
    player_partners = defaultdict(lambda: defaultdict(int))
    player_wins = defaultdict(int)
    player_losses = defaultdict(int)
    player_finals_won = defaultdict(list)
    player_finals_lost = defaultdict(list)
    player_events = defaultdict(set)

    combined_dir = DATA / "tournaments_combined"
    for f in sorted(os.listdir(combined_dir)):
        if not f.endswith(".json"):
            continue
        t = json.loads((combined_dir / f).read_text(encoding="utf-8"))
        tname = t.get("name", "")
        tdates = t.get("dates", "")

        for m in t.get("matches", []):
            for team_key, other_key, winner_num in [("team1", "team2", 1), ("team2", "team1", 2)]:
                for p in m.get(team_key, []):
                    uid = p.get("usab_id")
                    if not uid:
                        continue
                    player_events[uid].add((m.get("event", ""))[:2])
                    won = m.get("winner") == winner_num
                    if won:
                        player_wins[uid] += 1
                    else:
                        player_losses[uid] += 1

                    # Track partners
                    for partner in m.get(team_key, []):
                        if partner.get("usab_id") and partner["usab_id"] != uid:
                            player_partners[uid][partner.get("name", "")] += 1

                    # Track finals
                    if m.get("round") == "Final":
                        entry = f"{m.get('event','')} at {tname[:50]}"
                        if won:
                            player_finals_won[uid].append(entry)
                        else:
                            player_finals_lost[uid].append(entry)

        # Track tournament participation
        for uid_key, pinfo in t.get("players", {}).items():
            uid = pinfo.get("usab_id", uid_key)
            # Count matches in this tournament
            match_count = sum(
                1 for m in t.get("matches", [])
                for team in [m.get("team1", []), m.get("team2", [])]
                for p in team
                if p.get("usab_id") == uid
            )
            if match_count > 0:
                w = sum(
                    1 for m in t.get("matches", [])
                    for team_key, wn in [("team1", 1), ("team2", 2)]
                    for p in m.get(team_key, [])
                    if p.get("usab_id") == uid and m.get("winner") == wn
                )
                l = match_count - w
                player_tournaments[uid].append(f"{tdates:<20} {tname[:45]} {w}W-{l}L")

    # Load progression data
    prog_dir = DATA / "profiles_combined"
    prog_dates = sorted([f.replace(".json", "") for f in os.listdir(prog_dir) if f.endswith(".json")])
    # Sample a few dates for progression
    sample_dates = [prog_dates[0], prog_dates[len(prog_dates)//2], prog_dates[-1]] if len(prog_dates) >= 3 else prog_dates
    player_progression = defaultdict(list)
    for date in sample_dates:
        profiles = json.loads((prog_dir / f"{date}.json").read_text(encoding="utf-8"))
        for uid, prof in profiles.items():
            cats = prof.get("categories", [])
            if cats:
                rank_strs = [f"{c['category']} #{c['rank']}" for c in sorted(cats, key=lambda x: x['category'])]
                player_progression[uid].append(f"{date}: {', '.join(rank_strs)}")

    # Generate player data files
    all_uids = set(player_rankings.keys()) | set(k for k, v in player_wins.items() if v + player_losses.get(k, 0) >= 5)
    generated = 0

    for uid in sorted(all_uids):
        by = birth_years.get(uid, {})
        events = player_events.get(uid, set())
        if "GS" in events or "GD" in events:
            gender = "Female"
        elif "BS" in events or "BD" in events:
            gender = "Male"
        else:
            gender = by.get("gender", "Unknown")
            gender = "Female" if gender == "F" else "Male" if gender == "M" else "Unknown"

        w = player_wins.get(uid, 0)
        l = player_losses.get(uid, 0)
        total = w + l
        if total < 5:
            continue

        name = by.get("name") or "Unknown"
        if name == "Unknown":
            continue

        top_partners = sorted(player_partners.get(uid, {}).items(), key=lambda x: -x[1])[:5]

        data = {
            "name": name,
            "gender": gender,
            "yob": by.get("yob_inferred"),
            "rankings": player_rankings.get(uid, []),
            "career": f"{len(player_tournaments.get(uid, []))} tournaments, {w}-{l} ({round(100*w/total)}%)" if total else "",
            "progression": player_progression.get(uid, []),
            "recent": player_tournaments.get(uid, [])[-10:],
            "titles": player_finals_won.get(uid, []),
            "runner_ups": player_finals_lost.get(uid, []),
            "partners": [f"{n} ({c} matches)" for n, c in top_partners],
        }

        (PLAYER_DATA_DIR / f"{uid}.json").write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        generated += 1

    print(f"Generated {generated} player data files")
    return generated


def generate_summary(uid, data):
    """Generate a player profile summary using Claude CLI."""
    gender_pronoun = "she" if data["gender"] == "Female" else "he"
    prompt = f"""Write a compelling, positive, and unique player profile paragraph for a junior badminton player. Make it interesting and highlight what makes this player special. Use specific stats and tournament results. Write 3-5 <p> tag paragraphs, under 500 words total. No markdown formatting, just HTML <p> tags. Do not start with a generic opening.

Player data:
- Name: {data['name']}
- Gender: {data['gender']} (use {gender_pronoun}/{"her" if gender_pronoun == "she" else "him"} pronouns)
- Birth year: {data.get('yob', 'unknown')}
- Current rankings: {', '.join(data.get('rankings', [])) or 'unranked'}
- Career: {data.get('career', '')}
- Titles won: {', '.join(data.get('titles', [])) or 'none yet'}
- Finals lost: {', '.join(data.get('runner_ups', [])) or 'none'}
- Top doubles partners: {', '.join(data.get('partners', [])) or 'primarily singles'}
- Tournament history: {chr(10).join(data.get('recent', []))}
- Ranking progression: {chr(10).join(data.get('progression', []))}

Write ONLY the HTML paragraphs, nothing else."""

    result = subprocess.run(
        ["claude", "-p", prompt, "--model", "claude-sonnet-4-20250514"],
        capture_output=True, text=True, timeout=60, encoding="utf-8"
    )
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    else:
        print(f"  Summary error for {uid}: {result.stderr[:100]}")
        return None


def generate_roast(uid, data):
    """Generate a player roast using Claude CLI with a deterministic comedic style."""
    style = get_roast_style(data["name"])
    gender_pronoun = "she" if data["gender"] == "Female" else "he"

    prompt = f"""Write a comedic roast of a junior badminton player in the style of: {style}

Rules:
- Be funny and unique, fully commit to the format
- No generic openings
- Reference specific tournaments, partners, and results from the data
- Be mean but not cruel — punch up at winners, be gentler with underdogs
- Write 3-5 <p> tag paragraphs, under 500 words total
- No markdown, just HTML <p> tags

Player data:
- Name: {data['name']}
- Gender: {data['gender']} (use {gender_pronoun}/{"her" if gender_pronoun == "she" else "him"} pronouns)
- Birth year: {data.get('yob', 'unknown')}
- Current rankings: {', '.join(data.get('rankings', [])) or 'unranked'}
- Career: {data.get('career', '')}
- Titles won: {', '.join(data.get('titles', [])) or 'none yet'}
- Finals lost: {', '.join(data.get('runner_ups', [])) or 'none'}
- Top doubles partners: {', '.join(data.get('partners', [])) or 'primarily singles'}
- Tournament history: {chr(10).join(data.get('recent', []))}
- Ranking progression: {chr(10).join(data.get('progression', []))}

Write ONLY the HTML paragraphs, nothing else."""

    result = subprocess.run(
        ["claude", "-p", prompt, "--model", "claude-sonnet-4-20250514"],
        capture_output=True, text=True, timeout=60, encoding="utf-8"
    )
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    else:
        print(f"  Roast error for {uid}: {result.stderr[:100]}")
        return None


def process_one_player(args):
    """Process a single player — generate summary and/or roast."""
    uid, f, has_summary, has_roast = args
    data = json.loads((PLAYER_DATA_DIR / f).read_text(encoding="utf-8"))
    results = []

    if not has_summary:
        summary = generate_summary(uid, data)
        if summary:
            (SUMMARIES_DIR / f"{uid}.txt").write_text(summary, encoding="utf-8")
            results.append("summary")

    if not has_roast:
        roast = generate_roast(uid, data)
        if roast:
            (SUMMARIES_DIR / f"{uid}_roast.txt").write_text(roast, encoding="utf-8")
            results.append("roast")

    return uid, data["name"], results


def main():
    from concurrent.futures import ThreadPoolExecutor, as_completed

    WORKERS = int(sys.argv[1]) if len(sys.argv) > 1 else 10

    # Step 1: Build player data
    print("Building player data files...")
    build_player_data()

    # Step 2: Generate summaries and roasts
    files = sorted(os.listdir(PLAYER_DATA_DIR))
    total = len(files)

    # Filter to only players that need generation
    to_generate = []
    for f in files:
        uid = f.replace(".json", "")
        summary_exists = (SUMMARIES_DIR / f"{uid}.txt").exists()
        roast_exists = (SUMMARIES_DIR / f"{uid}_roast.txt").exists()
        if not summary_exists or not roast_exists:
            to_generate.append((uid, f, summary_exists, roast_exists))

    print(f"\n{len(to_generate)} players need generation (out of {total} total)")
    print(f"Using {WORKERS} parallel workers")

    done = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(process_one_player, args): args for args in to_generate}
        for future in as_completed(futures):
            done += 1
            try:
                uid, name, results = future.result()
                print(f"[{done}/{len(to_generate)}] {name} ({uid}) -> {', '.join(results) or 'failed'}", flush=True)
            except Exception as e:
                uid = futures[future][0]
                print(f"[{done}/{len(to_generate)}] {uid} -> ERROR: {e}", flush=True)

    print("\nDone!")


if __name__ == "__main__":
    main()
