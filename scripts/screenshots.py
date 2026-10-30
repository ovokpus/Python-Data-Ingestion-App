"""Headless Playwright screenshots of the running stack (docker compose up) -> docs/screenshots/."""
import json
import urllib.request
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).resolve().parent.parent / 'docs' / 'screenshots'
UI, API = 'http://localhost:8080', 'http://localhost:8000'


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        b = p.chromium.launch()
        pg = b.new_page(viewport={'width': 1280, 'height': 900}, device_scale_factor=2)
        pg.goto(UI)
        pg.wait_for_selector('.bar')
        pg.wait_for_timeout(800)
        pg.screenshot(path=OUT / 'dashboard.png')
        pg.screenshot(path=OUT / 'dashboard-full.png', full_page=True)
        pg.click('text=Sports Wire')
        pg.wait_for_function("document.querySelector('.panel h2:nth-of-type(1)') && [...document.querySelectorAll('h2')].some(h => h.textContent.includes('Sports Wire'))")
        pg.wait_for_timeout(2500)
        pg.locator('.grid').screenshot(path=OUT / 'entities-sports-wire.png')
        pg.goto(f'{API}/docs')
        pg.wait_for_selector('.opblock')
        pg.screenshot(path=OUT / 'api-docs.png')
        b.close()
    samples = {u: json.load(urllib.request.urlopen(API + u)) for u in
               ['/health', '/api/publications', '/api/entities?limit=5', '/api/flushes?limit=3', '/api/posts?limit=3']}
    (OUT / 'api-samples.json').write_text(json.dumps(samples, indent=2, default=str))


if __name__ == '__main__':
    main()
