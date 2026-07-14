from playwright.sync_api import sync_playwright
import json
with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    page = b.new_page()
    page.goto('https://datagolf.com/live-strokes-gained', wait_until='domcontentloaded', timeout=30000)
    page.wait_for_timeout(3000)
    data = page.evaluate("() => window.reload_data")
    if data:
        stats = data.get('stats') or {}
        print(f'stats keys: {sorted(stats.keys())}')
        # Sample one stat type
        for k in ('sg_total', 'sg_app', 'sg_ott'):
            if k in stats:
                rows = stats[k]
                print(f'{k}: list[{len(rows)}], first: {rows[0]}')
                break
        # sg_event
        sge = data.get('sg_event') or []
        print(f'sg_event: list[{len(sge)}], first: {sge[0] if sge else None}')
    b.close()
