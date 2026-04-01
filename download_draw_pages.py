"""Download all draw pages as raw HTML for offline parsing."""
import asyncio
import json
import os
import sys
from pathlib import Path
from playwright.async_api import async_playwright

BASE = "https://www.tournamentsoftware.com"
DATA = Path("data")
DRAW_PAGES_DIR = DATA / "draw_pages"
DRAW_PAGES_DIR.mkdir(exist_ok=True)


async def main():
    tids = sorted(os.listdir(DATA / "tournament_results"))

    # Filter to tournaments with draws.json
    to_download = []
    for tid in tids:
        out = DATA / "tournament_results" / tid
        draws_path = out / "draws.json"
        tourn_path = out / "tournament.json"
        if not draws_path.exists() or not tourn_path.exists():
            continue
        draws = json.loads(draws_path.read_text(encoding="utf-8"))
        t = json.loads(tourn_path.read_text(encoding="utf-8"))
        name = t.get("name", tid)[:55]

        # Check if already downloaded
        tid_dir = DRAW_PAGES_DIR / tid
        if tid_dir.exists():
            existing = len(list(tid_dir.glob("*.html")))
            if existing >= len(draws):
                continue  # Already have all pages

        to_download.append((tid, name, draws))

    print(f"Downloading draw pages for {len(to_download)} tournaments")
    print(f"(Skipping {len(tids) - len(to_download)} already downloaded)")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Accept cookies
        await page.goto(f"{BASE}/", wait_until="networkidle", timeout=30000)
        try:
            await page.click("button:has-text('Accept')", timeout=3000)
            await asyncio.sleep(0.5)
        except:
            pass

        for i, (tid, name, draws) in enumerate(to_download, 1):
            tid_dir = DRAW_PAGES_DIR / tid
            tid_dir.mkdir(exist_ok=True)

            downloaded = 0
            for d in draws:
                draw_id = d["draw_id"]
                draw_name = d["name"]
                html_path = tid_dir / f"draw_{draw_id}.html"
                text_path = tid_dir / f"draw_{draw_id}.txt"

                if html_path.exists() and text_path.exists():
                    downloaded += 1
                    continue

                url = f"{BASE}/sport/draw.aspx?id={tid}&draw={draw_id}"
                try:
                    await page.goto(url, wait_until="networkidle", timeout=30000)

                    # Scroll to load lazy content
                    for _ in range(3):
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await asyncio.sleep(1)

                    html = await page.content()
                    text = await (await page.query_selector("body")).inner_text()

                    html_path.write_text(html, encoding="utf-8")
                    text_path.write_text(text, encoding="utf-8")
                    downloaded += 1
                except Exception as e:
                    pass  # Skip failed pages

            print(f"[{i}/{len(to_download)}] {name}: {downloaded}/{len(draws)} pages", flush=True)

        await browser.close()

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
