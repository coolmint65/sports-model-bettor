from curl_cffi import requests as cc
# Try GET + POST on different paths with common shapes
candidates = [
    ('GET',  'https://apiweb.owgr.com/api/rankings'),
    ('GET',  'https://apiweb.owgr.com/api/rankings/current'),
    ('POST', 'https://apiweb.owgr.com/api/rankings/search'),
    ('POST', 'https://apiweb.owgr.com/api/rankings/search', {}),
    ('POST', 'https://apiweb.owgr.com/api/rankings/search',
     {'pageNumber': 1, 'pageSize': 200, 'sortColumn': 'CurrentRank', 'sortAscending': True}),
    ('GET',  'https://apiweb.owgr.com/api/rankings/getrankingsbyfilter'),
    ('GET',  'https://apiweb.owgr.com/api/rankingsbyfilter?eventId=0&dateID=0'),
]
for spec in candidates:
    method, url = spec[0], spec[1]
    body = spec[2] if len(spec) > 2 else None
    try:
        if method == 'GET':
            r = cc.get(url, impersonate='firefox133', timeout=10,
                        headers={'Origin': 'https://www.owgr.com',
                                 'Referer': 'https://www.owgr.com/'})
        else:
            r = cc.post(url, json=body or {}, impersonate='firefox133', timeout=10,
                         headers={'Origin': 'https://www.owgr.com',
                                  'Referer': 'https://www.owgr.com/',
                                  'Content-Type': 'application/json'})
        print(f'{method} {url[-50:]}: {r.status_code} len={len(r.content)}')
        if r.status_code == 200 and len(r.content) > 100:
            print(f'  body: {r.text[:300]}')
    except Exception as e:
        print(f'{method} {url[-50:]}: err {e}')
