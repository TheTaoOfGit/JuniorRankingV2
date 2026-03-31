"""Parse player positions directly from bracket HTML tables.
Much more accurate than match-based position detection.
"""
import json
import os
import re
from pathlib import Path
from collections import defaultdict

DATA = Path("data")
PAGES_DIR = DATA / "tournament_pages"


def parse_bracket_positions(tid, draw_id, draw_name, ts_to_usab):
    """Parse positions from the bracket table HTML for a draw.
    Returns dict: usab_id -> position (1, 2, 3, 4, 5, 9, 17, 33, etc.)
    """
    html_path = PAGES_DIR / tid / "draws" / f"draw_{draw_id}.html"
    if not html_path.exists():
        return {}

    html = html_path.read_text(encoding="utf-8")
    is_doubles = any(draw_name.startswith(x) for x in ("BD", "GD", "XD"))

    tables = re.findall(r"<table[^>]*>(.*?)</table>", html, re.DOTALL)

    positions = {}  # usab_id -> position

    main_table = None
    playoff_table = None

    for table_html in tables:
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.DOTALL)
        if len(rows) < 3:
            continue

        header_cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", rows[0], re.DOTALL)
        headers = [re.sub(r"<[^>]+>|&nbsp;", "", c).strip() for c in header_cells]

        if not any("Winner" in h for h in headers):
            continue

        # Get column indices
        round_cols = {}
        for i, h in enumerate(headers):
            if "Winner" in h:
                round_cols["Winner"] = i
            elif "Final" in h and "Semi" not in h and "Quarter" not in h:
                round_cols["Finals"] = i
            elif "Semi" in h:
                round_cols["SF"] = i
            elif "Quarter" in h:
                round_cols["QF"] = i
            elif "Round" in h:
                # Could be "Round 1", "Round 2", "Round of 32", etc.
                round_cols[h] = i

        # Determine if this is main bracket or 3rd/4th playoff
        is_playoff = len(round_cols) <= 3 and "Winner" in round_cols  # Small table = playoff

        # Get player UIDs per column
        col_uids = {}
        for col_name, col_idx in round_cols.items():
            uids = set()
            for row in rows[1:]:
                cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.DOTALL)
                if col_idx < len(cells):
                    for pm in re.finditer(r"player=(\d+)", cells[col_idx]):
                        uid = ts_to_usab.get(pm.group(1))
                        if uid:
                            uids.add(uid)
            col_uids[col_name] = uids

        if is_playoff and "Winner" in col_uids:
            # 3rd/4th playoff: Winner = 3rd, Finals-only = 4th
            winner_uids = col_uids.get("Winner", set())
            finals_uids = col_uids.get("Finals", set())
            for uid in winner_uids:
                positions[uid] = 3
            for uid in finals_uids - winner_uids:
                positions[uid] = 4
        else:
            # Main bracket
            winner_uids = col_uids.get("Winner", set())
            finals_uids = col_uids.get("Finals", set())
            sf_uids = col_uids.get("SF", set())
            qf_uids = col_uids.get("QF", set())

            # Collect round columns (Round 1, Round 2, etc.)
            round_uids_list = []
            for col_name in sorted(round_cols.keys()):
                if col_name not in ("Winner", "Finals", "SF", "QF"):
                    round_uids_list.append((col_name, col_uids.get(col_name, set())))

            for uid in winner_uids:
                positions[uid] = 1
            for uid in finals_uids - winner_uids:
                positions[uid] = 2
            for uid in sf_uids - finals_uids - winner_uids:
                if uid not in positions:
                    positions[uid] = 3  # Will be refined by playoff table
            for uid in qf_uids - sf_uids - finals_uids - winner_uids:
                positions[uid] = 5

            # Earlier round losers
            all_advanced = winner_uids | finals_uids | sf_uids | qf_uids
            prev_round_uids = set()
            # Process round columns from latest to earliest
            for col_name, uids in reversed(round_uids_list):
                losers = uids - all_advanced - prev_round_uids
                # Position depends on which round they're in
                # Use the column index to determine bracket depth
                col_idx = round_cols[col_name]
                winner_idx = round_cols.get("Winner", col_idx)
                rounds_from_winner = winner_idx - col_idx
                # Position = 2^(rounds_from_winner) + 1 for the earliest round
                # But more precisely: losers in this round = position based on round depth
                pos_map = {1: 9, 2: 17, 3: 33, 4: 65, 5: 129}
                pos = pos_map.get(rounds_from_winner, 2 ** rounds_from_winner + 1)
                for uid in losers:
                    if uid not in positions:
                        positions[uid] = pos
                prev_round_uids |= uids

    return positions


def build_all_bracket_positions():
    """Build bracket positions for all tournaments from saved HTML."""
    all_positions = {}  # (tid, event) -> {usab_id: position}

    for tid in sorted(os.listdir(PAGES_DIR)):
        tid_dir = PAGES_DIR / tid
        draws_dir = tid_dir / "draws"
        results_dir = DATA / "tournament_results" / tid

        if not draws_dir.exists() or not results_dir.exists():
            continue

        players_path = results_dir / "players.json"
        draws_path = results_dir / "draws.json"
        if not players_path.exists() or not draws_path.exists():
            continue

        players = json.loads(players_path.read_text(encoding="utf-8"))
        draws = json.loads(draws_path.read_text(encoding="utf-8"))
        ts_to_usab = {pid: p["usab_id"] for pid, p in players.items() if p.get("usab_id")}

        for d in draws:
            draw_id = d["draw_id"]
            draw_name = d["name"]
            event = draw_name.split(" - ")[0].strip()

            positions = parse_bracket_positions(tid, draw_id, draw_name, ts_to_usab)
            if positions:
                all_positions[(tid, event)] = positions

    return all_positions


def save_bracket_positions():
    """Save all bracket positions to a JSON file for use by ranking calculator."""
    all_positions = build_all_bracket_positions()

    # Convert to serializable format: {tid: {event: {usab_id: position}}}
    output = {}
    for (tid, event), positions in all_positions.items():
        if tid not in output:
            output[tid] = {}
        output[tid][event] = positions

    out_path = DATA / "bracket_positions.json"
    out_path.write_text(json.dumps(output, ensure_ascii=False), encoding="utf-8")

    total_players = sum(len(v) for tid_data in output.values() for v in tid_data.values())
    print(f"Saved bracket positions for {len(output)} tournaments, {total_players} player-positions")
    return output


if __name__ == "__main__":
    save_bracket_positions()
