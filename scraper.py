"""
USAB Junior Rankings Scraper
Scrapes all rankings and player profiles from usabjrrankings.org
Uses multiple concurrent browser tabs for speed.
"""

import asyncio
import json
import re
import sys
import time
from pathlib import Path
from playwright.async_api import async_playwright

BASE_URL = "https://usabjrrankings.org"
DATA_DIR = Path("data")
CATEGORIES = ["BS", "GS", "BD", "GD", "XD"]
AGE_GROUPS = ["U11", "U13", "U15", "U17", "U19"]

DATES = [
    "2026-03-01", "2026-02-01", "2026-01-01",
    "2025-12-01", "2025-10-31", "2025-10-01",
    "2025-08-01", "2025-06-01", "2025-05-01",
    "2025-04-01", "2025-03-01", "2025-02-01", "2025-01-01",
    "2024-12-01", "2024-11-01", "2024-09-02",
    "2024-08-01", "2024-05-01", "2024-04-01",
    "2024-03-01", "2024-01-01",
    "2023-12-31", "2023-12-01",
]

CONCURRENCY = 6  # number of parallel tabs

# Thread-safe progress tracking
progress_lock = asyncio.Lock()


def load_progress():
    progress_file = DATA_DIR / "progress.json"
    if progress_file.exists():
        return json.loads(progress_file.read_text(encoding="utf-8"))
    return {"rankings_done": set(), "profiles_done": set()}


def save_progress(progress):
    progress_file = DATA_DIR / "progress.json"
    data = {
        "rankings_done": sorted(progress["rankings_done"]),
        "profiles_done": sorted(progress["profiles_done"]),
    }
    progress_file.write_text(json.dumps(data), encoding="utf-8")


async def scrape_ranking(page, date, category, age_group):
    """Scrape a single ranking table."""
    url = f"{BASE_URL}/?category={category}&age_group={age_group}&date={date}"
    await page.goto(url, wait_until="networkidle", timeout=60000)

    try:
        await page.select_option("select[name='player-rankings_length']", "128", timeout=5000)
        await asyncio.sleep(1.5)
    except Exception:
        pass

    rows = await page.query_selector_all("#player-rankings tbody tr")
    rankings = []
    for row in rows:
        cells = await row.query_selector_all("td")
        if len(cells) < 4:
            continue
        text = [await c.inner_text() for c in cells]
        text = [t.strip() for t in text]
        if "No data" in text[0]:
            continue

        link_el = await cells[2].query_selector("a")
        player_url = await link_el.get_attribute("href") if link_el else None

        rankings.append({
            "rank": text[0],
            "usab_id": text[1],
            "name": text[2],
            "ranking_points": text[3],
            "profile_url": player_url,
        })

    return rankings


async def scrape_profile(page, usab_id, date):
    """Scrape a player's full profile for a given date."""
    url = f"{BASE_URL}/{usab_id}/details?age_group=U11&category=BS&date={date}"
    await page.goto(url, wait_until="networkidle", timeout=60000)

    body = await page.query_selector("body")
    if not body:
        return None

    text = await body.inner_text()

    profile = {"usab_id": usab_id, "date": date, "categories": []}

    m = re.search(r"Player Details\s*\n(.+?)\s*\(USAB#", text)
    if m:
        profile["name"] = m.group(1).strip()

    m = re.search(r"Year Of Birth\s*:\s*(\d{4})?", text)
    if m:
        profile["year_of_birth"] = m.group(1) if m.group(1) else None

    m = re.search(r"Gender\s*:\s*([MF])", text)
    if m:
        profile["gender"] = m.group(1).strip()

    sections = re.split(r"\n([A-Z]{2} U\d{2})\n", text)
    for i in range(1, len(sections) - 1, 2):
        cat_name = sections[i].strip()
        content = sections[i + 1]
        cat_data = {"category": cat_name}

        m = re.search(r"Ranking Points \(Rank\)\s*:\s*([\d,]+)\s*\(\s*(\d+)\s*\)", content)
        if m:
            cat_data["ranking_points"] = int(m.group(1).replace(",", ""))
            cat_data["rank"] = int(m.group(2))

        tournaments = []
        for line in content.strip().split("\n"):
            if line.startswith("Ranking Points") or line.startswith("Tournament Name"):
                continue
            parts = line.split("\t")
            if len(parts) == 3:
                tournaments.append({
                    "tournament": parts[0].strip(),
                    "position": parts[1].strip(),
                    "points": parts[2].strip(),
                })
        cat_data["tournaments"] = tournaments
        profile["categories"].append(cat_data)

    return profile


async def worker(sem, page, task_fn):
    """Run a task with concurrency limiting."""
    async with sem:
        return await task_fn(page)


async def main():
    DATA_DIR.mkdir(exist_ok=True)
    (DATA_DIR / "rankings").mkdir(exist_ok=True)
    (DATA_DIR / "profiles").mkdir(exist_ok=True)

    progress = load_progress()
    # Convert lists to sets for fast lookup
    progress["rankings_done"] = set(progress["rankings_done"])
    progress["profiles_done"] = set(progress["profiles_done"])

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        # Phase 1: Scrape all ranking tables
        print("=== Phase 1: Scraping ranking tables ===")
        all_tasks = []
        for date in DATES:
            for category in CATEGORIES:
                for age_group in AGE_GROUPS:
                    key = f"{date}_{category}_{age_group}"
                    if key not in progress["rankings_done"]:
                        all_tasks.append((date, category, age_group, key))

        print(f"Rankings to scrape: {len(all_tasks)} (skipping {len(progress['rankings_done'])} already done)")

        sem = asyncio.Semaphore(CONCURRENCY)
        # Process in batches using a pool of pages
        pages = [await browser.new_page() for _ in range(CONCURRENCY)]
        batch_size = CONCURRENCY
        total = len(all_tasks)
        done_count = 0

        for batch_start in range(0, total, batch_size):
            batch = all_tasks[batch_start:batch_start + batch_size]
            tasks = []
            for i, (date, category, age_group, key) in enumerate(batch):
                page = pages[i % len(pages)]

                async def do_ranking(p=page, d=date, c=category, ag=age_group, k=key):
                    rankings = await scrape_ranking(p, d, c, ag)
                    result = {"date": d, "category": c, "age_group": ag, "rankings": rankings}
                    fpath = DATA_DIR / "rankings" / f"{k}.json"
                    fpath.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
                    return k, rankings

                tasks.append(do_ranking())

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for r in results:
                done_count += 1
                if isinstance(r, Exception):
                    print(f"  [{done_count}/{total}] ERROR: {r}")
                else:
                    key, rankings = r
                    progress["rankings_done"].add(key)
                    player_count = len(rankings)
                    print(f"  [{done_count}/{total}] {key}: {player_count} players")

            save_progress(progress)

        # Close ranking pages
        for pg in pages:
            await pg.close()

        # Phase 2: Collect all unique player IDs per date
        print("\n=== Phase 2: Scraping player profiles ===")
        all_profile_tasks = []
        for date in DATES:
            player_ids = set()
            for category in CATEGORIES:
                for age_group in AGE_GROUPS:
                    key = f"{date}_{category}_{age_group}"
                    fpath = DATA_DIR / "rankings" / f"{key}.json"
                    if fpath.exists():
                        data = json.loads(fpath.read_text(encoding="utf-8"))
                        for r in data.get("rankings", []):
                            player_ids.add(r["usab_id"])

            date_dir = DATA_DIR / "profiles" / date
            date_dir.mkdir(exist_ok=True)

            for usab_id in sorted(player_ids):
                profile_key = f"{date}_{usab_id}"
                fpath = date_dir / f"{usab_id}.json"
                if profile_key not in progress["profiles_done"] and not fpath.exists():
                    all_profile_tasks.append((date, usab_id, profile_key))

        print(f"Profiles to scrape: {len(all_profile_tasks)} (skipping {len(progress['profiles_done'])} already done)")

        pages = [await browser.new_page() for _ in range(CONCURRENCY)]
        total = len(all_profile_tasks)
        done_count = 0

        for batch_start in range(0, total, batch_size):
            batch = all_profile_tasks[batch_start:batch_start + batch_size]
            tasks = []
            for i, (date, usab_id, profile_key) in enumerate(batch):
                page = pages[i % len(pages)]

                async def do_profile(p=page, d=date, uid=usab_id, pk=profile_key):
                    profile = await scrape_profile(p, uid, d)
                    fpath = DATA_DIR / "profiles" / d / f"{uid}.json"
                    fpath.write_text(json.dumps(profile, indent=2, ensure_ascii=False), encoding="utf-8")
                    return pk, profile.get("name", uid) if profile else uid

                tasks.append(do_profile())

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for r in results:
                done_count += 1
                if isinstance(r, Exception):
                    print(f"  [{done_count}/{total}] ERROR: {r}")
                else:
                    pk, name = r
                    progress["profiles_done"].add(pk)
                    if done_count % 20 == 0 or done_count == total:
                        print(f"  [{done_count}/{total}] Last: {name}")

            save_progress(progress)

        for pg in pages:
            await pg.close()
        await browser.close()

    print("\nDone! All data saved to ./data/")


if __name__ == "__main__":
    asyncio.run(main())
