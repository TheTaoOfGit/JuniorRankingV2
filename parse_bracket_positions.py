"""Parse player positions from bracket HTML tables using BeautifulSoup.
Handles rowspan correctly for accurate position extraction.
"""
import json
import os
import re
from pathlib import Path
from bs4 import BeautifulSoup

DATA = Path("data")
PAGES_DIR = DATA / "tournament_pages"


def parse_bracket_positions(tid, draw_id, draw_name, ts_to_usab):
    """Parse positions from bracket HTML table. Returns dict: usab_id -> position."""
    html_path = PAGES_DIR / tid / "draws" / f"draw_{draw_id}.html"
    if not html_path.exists():
        return {}

    html = html_path.read_text(encoding="utf-8")
    soup = BeautifulSoup(html, "html.parser")

    positions = {}

    for table in soup.find_all("table"):
        header_row = table.find("tr")
        if not header_row:
            continue
        headers = [td.get_text(strip=True) for td in header_row.find_all(["td", "th"])]

        # Determine if this is a 3rd/4th playoff table using <caption>
        caption = table.find("caption")
        caption_text = caption.get_text(strip=True) if caption else ""
        is_playoff = "3/4" in caption_text or "3-4" in caption_text or "3rd" in caption_text

        # Map column names to indices
        col_map = {}
        for i, h in enumerate(headers):
            if "Winner" in h:
                col_map["Winner"] = i
            elif "Final" in h and "Semi" not in h and "Quarter" not in h:
                col_map["Finals"] = i
            elif "Semi" in h:
                col_map["SF"] = i
            elif "Quarter" in h:
                col_map["QF"] = i
            elif re.match(r"Round \d+$|Round of \d+$", h):
                col_map[h] = i

        # For playoff tables without 'Winner' header, use last column as winner
        if "Winner" not in col_map and is_playoff and len(headers) >= 2:
            col_map["Winner"] = len(headers) - 1
            if len(headers) >= 3:
                col_map["Finals"] = len(headers) - 2

        if "Winner" not in col_map:
            continue

        # Build grid with rowspan handling
        rows = table.find_all("tr")
        grid = []
        rowspan_tracker = {}

        for row in rows:
            cells = row.find_all(["td", "th"])
            row_data = []
            ci = 0
            cell_idx = 0

            while ci < len(headers):
                if ci in rowspan_tracker and rowspan_tracker[ci][0] > 0:
                    row_data.append(rowspan_tracker[ci][1])
                    rowspan_tracker[ci] = (rowspan_tracker[ci][0] - 1, rowspan_tracker[ci][1])
                    ci += 1
                elif cell_idx < len(cells):
                    cell = cells[cell_idx]
                    rs = int(cell.get("rowspan", 1))
                    cs = int(cell.get("colspan", 1))

                    uids = set()
                    for a in cell.find_all("a", href=True):
                        pm = re.search(r"player=(\d+)", a["href"])
                        if pm:
                            uid = ts_to_usab.get(pm.group(1))
                            if uid:
                                uids.add(uid)

                    cell_data = uids
                    row_data.append(cell_data)

                    if rs > 1:
                        rowspan_tracker[ci] = (rs - 1, cell_data)

                    for _ in range(1, cs):
                        ci += 1
                        row_data.append(cell_data)

                    ci += 1
                    cell_idx += 1
                else:
                    row_data.append(set())
                    ci += 1

            grid.append(row_data)

        # Extract UIDs per column
        def get_col_uids(col_name):
            if col_name not in col_map:
                return set()
            idx = col_map[col_name]
            uids = set()
            for row in grid[1:]:  # skip header
                if idx < len(row):
                    uids |= row[idx]
            return uids

        winner_uids = get_col_uids("Winner")
        finals_uids = get_col_uids("Finals")
        sf_uids = get_col_uids("SF")
        qf_uids = get_col_uids("QF")

        # Collect round columns
        round_col_uids = []
        for col_name in sorted(col_map.keys(), key=lambda x: col_map[x], reverse=True):
            if col_name not in ("Winner", "Finals", "SF", "QF"):
                round_col_uids.append((col_name, col_map[col_name], get_col_uids(col_name)))

        if is_playoff:
            # 3rd/4th playoff table
            for uid in winner_uids:
                positions[uid] = 3
            for uid in finals_uids - winner_uids:
                positions[uid] = 4
        else:
            # Main bracket
            for uid in winner_uids:
                positions[uid] = 1
            for uid in finals_uids - winner_uids:
                positions[uid] = 2
            # SF losers: 3 if no playoff table found later, else will be overridden
            for uid in sf_uids - finals_uids - winner_uids:
                if uid not in positions:
                    positions[uid] = 3
            for uid in qf_uids - sf_uids - finals_uids - winner_uids:
                if uid not in positions:
                    positions[uid] = 5

            # Earlier round losers
            all_advanced = winner_uids | finals_uids | sf_uids | qf_uids
            winner_col_idx = col_map["Winner"]

            for col_name, col_idx, col_uids_set in round_col_uids:
                rounds_from_winner = winner_col_idx - col_idx
                # Position for losers in this round
                pos = 2 ** rounds_from_winner + 1
                for uid in col_uids_set - all_advanced:
                    if uid not in positions:
                        positions[uid] = pos
                all_advanced |= col_uids_set

    return positions


def save_bracket_positions():
    """Save all bracket positions to JSON."""
    output = {}

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

        tid_data = {}
        for d in draws:
            draw_id = d["draw_id"]
            draw_name = d["name"]
            event = draw_name.split(" - ")[0].strip()

            positions = parse_bracket_positions(tid, draw_id, draw_name, ts_to_usab)
            if positions:
                tid_data[event] = positions

        if tid_data:
            output[tid] = tid_data

    out_path = DATA / "bracket_positions.json"
    out_path.write_text(json.dumps(output, ensure_ascii=False), encoding="utf-8")

    total_players = sum(len(v) for tid_data in output.values() for v in tid_data.values())
    print(f"Saved bracket positions for {len(output)} tournaments, {total_players} player-positions")


if __name__ == "__main__":
    save_bracket_positions()
