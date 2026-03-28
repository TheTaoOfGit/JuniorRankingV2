"""
Validate scraped tournament data by:
1. Extracting bracket winners from each draw's Winner column
2. Cross-checking Final match winners against bracket winners
3. Verifying score consistency (winner of match = player who won more games)
4. Checking team sizes (singles=1, doubles=2)
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


async def get_bracket_winners(page, tid, draws, ts_to_usab):
    """Extract the winner from each draw's bracket Winner column."""
    winners = {}  # draw_name -> {name, usab_id}

    for d in draws:
        # Skip group draws (they feed into main draw)
        if "Group" in d["name"]:
            continue

        url = f"{BASE}/sport/draw.aspx?id={tid}&draw={d['draw_id']}"
        await page.goto(url, wait_until="networkidle", timeout=30000)

        content_el = await page.query_selector("#content")
        if not content_el:
            continue

        tables = await content_el.query_selector_all("table")
        winner_name = None
        winner_usab = None

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
                        usab_id = ts_to_usab.get(pid_m.group(1))
                        winner_name = name
                        winner_usab = usab_id

        if winner_name:
            winners[d["name"]] = {"name": winner_name, "usab_id": winner_usab}
            # For doubles, get partner too
            is_doubles = any(d["name"].startswith(x) for x in ("BD", "GD", "XD", "Doubles"))
            if is_doubles:
                # Find all player links in the winner column
                partner_names = []
                for table in tables:
                    rows = await table.query_selector_all("tr")
                    for row in rows:
                        cells = await row.query_selector_all("td")
                        if len(cells) < 7:
                            continue
                        last_cell = cells[-1]
                        links = await last_cell.query_selector_all("a[href*='player=']")
                        for lnk in links:
                            n = (await lnk.inner_text()).strip()
                            n = re.sub(r"\s*\[\d+(?:/\d+)?\]", "", n).strip()
                            h = await lnk.get_attribute("href") or ""
                            pm = re.search(r"player=(\d+)", h)
                            if pm:
                                partner_names.append({"name": n, "usab_id": ts_to_usab.get(pm.group(1))})
                if len(partner_names) >= 2:
                    winners[d["name"]] = partner_names[:2]

        print(f"  {d['name']}: winner = {winner_name or 'N/A'}")

    return winners


def validate_matches(matches, bracket_winners):
    """Run all validation checks on parsed matches."""
    errors = []
    warnings = []

    for i, m in enumerate(matches):
        event = m["event"]
        rnd = m.get("round", "")
        t1 = m.get("team1", [])
        t2 = m.get("team2", [])
        scores = m.get("scores", [])
        winner = m.get("winner")
        is_walkover = m.get("walkover", False)
        is_doubles = any(event.startswith(x) for x in ("BD", "GD", "XD", "Doubles"))

        # CHECK 1: Team sizes
        expected_size = 2 if is_doubles else 1
        if not is_walkover:
            if len(t1) != expected_size:
                warnings.append(f"Match {i} ({event} {rnd}): team1 has {len(t1)} players, expected {expected_size}")
            if len(t2) != expected_size:
                warnings.append(f"Match {i} ({event} {rnd}): team2 has {len(t2)} players, expected {expected_size}")

        # CHECK 2: Score consistency - winner should have won more games
        if scores and winner and not is_walkover:
            t1_games = sum(1 for s in scores if s[0] > s[1])
            t2_games = sum(1 for s in scores if s[1] > s[0])
            score_winner = 1 if t1_games > t2_games else 2 if t2_games > t1_games else None

            if score_winner and score_winner != winner:
                errors.append(
                    f"Match {i} ({event} {rnd}): W marker says team{winner} won, "
                    f"but scores {scores} say team{score_winner} won. "
                    f"T1={[p['name'] for p in t1]} vs T2={[p['name'] for p in t2]}"
                )

        # CHECK 3: Valid badminton scores
        for s in scores:
            if len(s) != 2:
                errors.append(f"Match {i} ({event} {rnd}): invalid score pair {s}")
                continue
            s1, s2 = s
            if s1 > 30 or s2 > 30:
                errors.append(f"Match {i} ({event} {rnd}): score {s1}-{s2} exceeds 30")
            if s1 < 21 and s2 < 21:
                warnings.append(f"Match {i} ({event} {rnd}): neither side reached 21 ({s1}-{s2})")
            if (s1 == 21 or s2 == 21) and abs(s1 - s2) < 2 and max(s1, s2) == 21:
                # 21-20 is not valid in badminton (need 2 point lead after 20-20)
                if min(s1, s2) == 20:
                    errors.append(f"Match {i} ({event} {rnd}): score 21-20 is invalid in badminton")

    # CHECK 4: Final match winners vs bracket winners
    for m in matches:
        if m.get("round") == "Final":
            draw_name = m.get("draw", m["event"])
            event_name = m["event"]
            bracket_key = None
            for k in bracket_winners:
                if k.startswith(event_name) and "Group" not in k:
                    bracket_key = k
                    break

            if bracket_key and bracket_key in bracket_winners:
                bw = bracket_winners[bracket_key]
                winning_team = m.get("team1") if m.get("winner") == 1 else m.get("team2") if m.get("winner") == 2 else []
                winning_ids = {p.get("usab_id") for p in winning_team if p.get("usab_id")}

                if isinstance(bw, list):
                    bracket_ids = {p.get("usab_id") for p in bw if p.get("usab_id")}
                else:
                    bracket_ids = {bw.get("usab_id")} if bw.get("usab_id") else set()

                if bracket_ids and winning_ids and not bracket_ids.issubset(winning_ids):
                    errors.append(
                        f"FINAL MISMATCH {event_name}: match says winner={[p['name'] for p in winning_team]}, "
                        f"but bracket says winner={bw}"
                    )
                elif bracket_ids and winning_ids and bracket_ids == winning_ids:
                    pass  # Validated!

    return errors, warnings


async def main():
    tid = "5779DD58-5F08-4D64-A092-B41478B07A0A"
    out = Path(f"data/tournament_results/{tid}")

    players = json.loads((out / "players.json").read_text(encoding="utf-8"))
    draws = json.loads((out / "draws.json").read_text(encoding="utf-8"))
    matches = json.loads((out / "matches.json").read_text(encoding="utf-8"))

    ts_to_usab = {pid: p["usab_id"] for pid, p in players.items() if p.get("usab_id")}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Accept cookies
        await page.goto(f"{BASE}/sport/draws.aspx?id={tid}", wait_until="networkidle", timeout=30000)
        try:
            await page.click("button:has-text('Accept')", timeout=3000)
            await asyncio.sleep(0.5)
        except:
            pass

        # Get bracket winners
        print("=== Extracting bracket winners ===")
        bracket_winners = await get_bracket_winners(page, tid, draws, ts_to_usab)

        await browser.close()

    # Save bracket winners
    (out / "bracket_winners.json").write_text(
        json.dumps(bracket_winners, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    # Validate
    print("\n=== Running validation ===")
    errors, warnings = validate_matches(matches, bracket_winners)

    print(f"\nERRORS: {len(errors)}")
    for e in errors:
        print(f"  !! {e}")

    print(f"\nWARNINGS: {len(warnings)}")
    for w in warnings[:20]:
        print(f"  ?? {w}")
    if len(warnings) > 20:
        print(f"  ... and {len(warnings) - 20} more warnings")

    # Save validation report
    report = {"errors": errors, "warnings": warnings, "bracket_winners": bracket_winners}
    (out / "validation_report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    print(f"\nValidation report saved to {out}/validation_report.json")


if __name__ == "__main__":
    asyncio.run(main())
