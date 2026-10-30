"""Builds data/sample_posts.jsonl: real text (intro extracts of English Wikipedia articles, CC BY-SA 4.0)
grouped under made-up demo publication names. Run once; the output is committed."""
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

PUBLICATIONS = {
    "Tech Ledger": ["Apple Inc.", "Microsoft", "Nvidia", "OpenAI", "Linux", "Python (programming language)",
                    "Google", "Amazon (company)", "Tesla, Inc.", "ARM architecture family"],
    "World Dispatch": ["United Nations", "European Union", "Canada", "Nigeria", "Japan", "Brazil",
                       "African Union", "NATO", "India", "Australia"],
    "Science Weekly": ["NASA", "James Webb Space Telescope", "CERN", "Large Hadron Collider", "Marie Curie",
                       "Albert Einstein", "CRISPR", "Mars", "International Space Station", "Charles Darwin"],
    "Sports Wire": ["FIFA World Cup", "Olympic Games", "National Basketball Association", "Wayne Gretzky",
                    "Serena Williams", "Lionel Messi", "Tour de France", "Wimbledon Championships",
                    "Edmonton Oilers", "Super Bowl"],
}
API = "https://en.wikipedia.org/w/api.php"
UA = {"User-Agent": "Python-Data-Ingestion-App sample builder (portfolio demo)"}


def extracts(titles: list[str]) -> dict[str, tuple[str, str]]:
    """One request per publication (up to 20 titles), retrying politely on HTTP 429."""
    q = urllib.parse.urlencode({"action": "query", "prop": "extracts|info", "exintro": 1, "explaintext": 1,
                                "exlimit": 20, "inprop": "url", "redirects": 1, "titles": "|".join(titles),
                                "format": "json", "formatversion": 2})
    for attempt in range(6):
        try:
            with urllib.request.urlopen(urllib.request.Request(f"{API}?{q}", headers=UA), timeout=30) as r:
                data = json.load(r)["query"]
            break
        except urllib.error.HTTPError as e:
            if e.code != 429:
                raise
            time.sleep(5 * (attempt + 1))
    else:
        raise SystemExit("Wikipedia kept returning 429")
    alias = {x["to"]: x["from"] for x in data.get("redirects", []) + data.get("normalized", [])}
    return {alias.get(p["title"], p["title"]): (p["extract"].strip(), p["fullurl"]) for p in data["pages"]}


out = Path(__file__).resolve().parent.parent / "data" / "sample_posts.jsonl"
with out.open("w") as f:
    for pub, titles in PUBLICATIONS.items():
        got = extracts(titles)
        for t in titles:
            text, url = got[t]
            f.write(json.dumps({"publication": pub, "title": t, "source_url": url, "content": text}) + "\n")
        time.sleep(2)
print(out, sum(1 for _ in out.open()))
