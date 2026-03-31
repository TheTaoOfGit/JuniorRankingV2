"""Download all pages for each tournament for offline parsing.
Saves: overview, players list, draws list, each draw page, each player page.
Resumable - skips already downloaded pages.
"""
import asyncio
import json
import os
import re
import sys
from pathlib import Path
from playwright.async_api import async_playwright

BASE = "https://www.tournamentsoftware.com"
DATA = Path("data")
PAGES_DIR = DATA / "tournament_pages"
PAGES_DIR.mkdir(exist_ok=True)
CONCURRENCY = 24


async def save_page(page, url, html_path, text_path, scroll=False):
    """Navigate to URL, optionally scroll, save HTML and text."""
    if html_path.exists() and text_path.exists():
        return True  # Already saved

    try:
        await page.goto(url, wait_until="networkidle", timeout=30000)
        if scroll:
            for _ in range(3):
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(1)
        html = await page.content()
        body = await page.query_selector("body")
        text = await body.inner_text() if body else ""
        html_path.write_text(html, encoding="utf-8")
        text_path.write_text(text, encoding="utf-8")
        return True
    except Exception as e:
        return False


async def download_tournament(browser, tid, name, draws, player_ids):
    """Download all pages for a single tournament."""
    tid_dir = PAGES_DIR / tid
    tid_dir.mkdir(exist_ok=True)

    page = await browser.new_page()

    # Accept cookies
    try:
        await page.goto(f"{BASE}/", wait_until="networkidle", timeout=15000)
        await page.click("button:has-text('Accept')", timeout=2000)
        await asyncio.sleep(0.3)
    except:
        pass

    saved = {"overview": 0, "players": 0, "draws_list": 0, "draws": 0, "player_pages": 0}

    # 1. Overview page
    ok = await save_page(
        page,
        f"{BASE}/tournament/{tid}",
        tid_dir / "overview.html",
        tid_dir / "overview.txt",
    )
    if ok:
        saved["overview"] = 1

    # 2. Players list page
    ok = await save_page(
        page,
        f"{BASE}/sport/players.aspx?id={tid}",
        tid_dir / "players.html",
        tid_dir / "players.txt",
    )
    if ok:
        saved["players"] = 1

    # 3. Draws list page
    ok = await save_page(
        page,
        f"{BASE}/sport/draws.aspx?id={tid}",
        tid_dir / "draws.html",
        tid_dir / "draws.txt",
    )
    if ok:
        saved["draws_list"] = 1

    # 4. Each draw page (with scroll for lazy-loaded matches)
    draws_dir = tid_dir / "draws"
    draws_dir.mkdir(exist_ok=True)
    for d in draws:
        draw_id = d["draw_id"]
        ok = await save_page(
            page,
            f"{BASE}/sport/draw.aspx?id={tid}&draw={draw_id}",
            draws_dir / f"draw_{draw_id}.html",
            draws_dir / f"draw_{draw_id}.txt",
            scroll=True,
        )
        if ok:
            saved["draws"] += 1

    await page.close()

    # 5. Player pages (concurrent with multiple tabs)
    players_dir = tid_dir / "players"
    players_dir.mkdir(exist_ok=True)

    # Filter to players not yet downloaded
    to_download = []
    for pid in player_ids:
        html_p = players_dir / f"player_{pid}.html"
        text_p = players_dir / f"player_{pid}.txt"
        if not html_p.exists() or not text_p.exists():
            to_download.append(pid)
        else:
            saved["player_pages"] += 1

    if to_download:
        sem = asyncio.Semaphore(CONCURRENCY)
        pages_pool = [await browser.new_page() for _ in range(min(CONCURRENCY, len(to_download)))]

        async def fetch_player(pg, pid):
            async with sem:
                ok = await save_page(
                    pg,
                    f"{BASE}/sport/player.aspx?id={tid}&player={pid}",
                    players_dir / f"player_{pid}.html",
                    players_dir / f"player_{pid}.txt",
                )
                return ok

        for batch_start in range(0, len(to_download), CONCURRENCY):
            batch = to_download[batch_start:batch_start + CONCURRENCY]
            tasks = [fetch_player(pages_pool[i % len(pages_pool)], pid) for i, pid in enumerate(batch)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            saved["player_pages"] += sum(1 for r in results if r is True)

        for pg in pages_pool:
            await pg.close()

    return saved


async def main():
    tids = sorted(os.listdir(DATA / "tournament_results"))

    to_download = []
    for tid in tids:
        out = DATA / "tournament_results" / tid
        draws_path = out / "draws.json"
        players_path = out / "players.json"
        tourn_path = out / "tournament.json"
        if not tourn_path.exists():
            continue

        draws = json.loads(draws_path.read_text(encoding="utf-8")) if draws_path.exists() else []
        players = json.loads(players_path.read_text(encoding="utf-8")) if players_path.exists() else {}
        t = json.loads(tourn_path.read_text(encoding="utf-8"))
        name = t.get("name", tid)[:55]
        player_ids = sorted(players.keys())

        # Check if fully downloaded
        tid_dir = PAGES_DIR / tid
        if tid_dir.exists():
            draws_done = len(list((tid_dir / "draws").glob("*.html"))) if (tid_dir / "draws").exists() else 0
            players_done = len(list((tid_dir / "players").glob("*.html"))) if (tid_dir / "players").exists() else 0
            if draws_done >= len(draws) and players_done >= len(player_ids):
                continue  # Fully downloaded

        to_download.append((tid, name, draws, player_ids))

    print(f"Downloading pages for {len(to_download)} tournaments")
    print(f"(Skipping {len(tids) - len(to_download)} fully downloaded)")

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

        for i, (tid, name, draws, player_ids) in enumerate(to_download, 1):
            try:
                saved = await download_tournament(browser, tid, name, draws, player_ids)
                parts = f"ov={saved['overview']} pl={saved['players']} dr={saved['draws']}/{len(draws)} pp={saved['player_pages']}/{len(player_ids)}"
                print(f"[{i}/{len(to_download)}] {name}: {parts}", flush=True)
            except Exception as e:
                print(f"[{i}/{len(to_download)}] {name}: ERROR {str(e)[:60]}", flush=True)

        await browser.close()

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
