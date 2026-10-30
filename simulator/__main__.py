'''Sends data/sample_posts.jsonl to the ingestion front-end (POST /post/enqueue).

The text is real (English Wikipedia intro extracts, CC BY-SA 4.0); the publication names are made up for the demo.
    python -m simulator --url http://localhost:8000 --rate 10 --repeat 1
'''
import argparse
import json
import os
import time
import urllib.request
from pathlib import Path

DATA = Path(__file__).resolve().parent.parent / 'data' / 'sample_posts.jsonl'


def wait_for(url: str, timeout: float = 120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f'{url}/health', timeout=3) as r:
                if json.load(r).get('queue'):
                    return
        except Exception:
            pass
        time.sleep(1)
    raise SystemExit(f'ingestion API at {url} not ready')


def send(url: str, post: dict, api_key: str = None) -> dict:
    headers = {'Content-Type': 'application/json', **({'access_token': api_key} if api_key else {})}
    body = json.dumps({k: post[k] for k in ('publication', 'title', 'content')}).encode()
    req = urllib.request.Request(f'{url}/post/enqueue', data=body, headers=headers, method='POST')
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--url', default=os.environ.get('INGEST_URL', 'http://localhost:8000'))
    ap.add_argument('--rate', type=float, default=float(os.environ.get('SIM_RATE', '8')), help='posts per second')
    ap.add_argument('--repeat', type=int, default=int(os.environ.get('SIM_REPEAT', '1')))
    a = ap.parse_args()
    posts = [json.loads(line) for line in DATA.open()]
    wait_for(a.url)
    n = 0
    for _ in range(a.repeat):
        for p in posts:
            send(a.url, p, os.environ.get('INGEST_API_KEY'))
            n += 1
            time.sleep(1 / a.rate)
    print(f'sent {n} posts')


if __name__ == '__main__':
    main()
