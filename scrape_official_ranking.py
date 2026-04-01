"""Scrape official rankings from usabjrrankings.org for a specific date."""
import asyncio
import json
import sys
from pathlib import Path
from scraper import scrape_ranking, BASE_URL, DATA_DIR, CATEGORIES, AGE_GROUPS

CONCURRENCY = 6


async def main():
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-04-01"
    print(f"Scraping official rankings for {date}")

    # Load existing combined rankings
    combined_path = DATA_DIR / "rankings_combined.json"
    if combined_path.exists():
        combined = json.loads(combined_path.read_text(encoding="utf-8"))
    else:
        combined = {}

    from playwright.async_api import async_playwright
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        pages = [await browser.new_page() for _ in range(CONCURRENCY)]

        tasks_list = []
        for cat in CATEGORIES:
            for ag in AGE_GROUPS:
                tasks_list.append((cat, ag))

        done = 0
        for i in range(0, len(tasks_list), CONCURRENCY):
            batch = tasks_list[i:i + CONCURRENCY]
            coros = []
            for j, (cat, ag) in enumerate(batch):
                pg = pages[j % CONCURRENCY]
                coros.append(scrape_ranking(pg, date, cat, ag))

            results = await asyncio.gather(*coros, return_exceptions=True)

            for (cat, ag), result in zip(batch, results):
                done += 1
                if isinstance(result, Exception):
                    print(f"  [{done}/{len(tasks_list)}] {cat} {ag}: ERROR {result}", flush=True)
                    continue
                key = f"{date}_{cat}_{ag}"
                combined[key] = {"rankings": result}
                print(f"  [{done}/{len(tasks_list)}] {cat} {ag}: {len(result)} players", flush=True)

        for pg in pages:
            await pg.close()
        await browser.close()

    # Save
    combined_path.write_text(json.dumps(combined, ensure_ascii=False), encoding="utf-8")
    print(f"\nSaved to {combined_path}")


if __name__ == "__main__":
    asyncio.run(main())
