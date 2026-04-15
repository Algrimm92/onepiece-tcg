"""
One Piece TCG — Box Price Scraper
===================================
Scrapes sealed product (booster box) prices from PriceCharting.
Run this monthly. Takes ~3 minutes.

Usage:  python scraper_boxes.py
Output: one_piece_prices.csv
"""
import csv, json, re, os, sys, time, random
from datetime import datetime, timezone
try:
    from curl_cffi import requests
    CURL_MODE = True
except ImportError:
    import requests
    CURL_MODE = False

os.chdir(os.path.expanduser("~/TCG/onepiece/exports"))

OUTPUT = "one_piece_prices.csv"
HEADERS_CSV = ["date","set_id","set_name","product","price_usd","msrp","set_type"]
DEBUG = len(sys.argv) > 1 and sys.argv[1].lower() == "debug"

if CURL_MODE:
    SESSION = requests.Session(impersonate="chrome120")
    print("  Using curl_cffi Chrome impersonation mode")
else:
    SESSION = requests.Session()
    SESSION.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "max-age=0",
        "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"macOS"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Upgrade-Insecure-Requests": "1",
    })
    print("  curl_cffi not installed — using standard requests")

SETS = [
    {"id":"OP01","name":"Romance Dawn","slug":"one-piece-romance-dawn","release_date":"2022-12-02","msrp":72.0,"type":"main","products":["booster-box","blue-bottom-booster-box"]},
    {"id":"OP02","name":"Paramount War","slug":"one-piece-paramount-war","release_date":"2023-03-10","msrp":72.0,"type":"main","products":["booster-box"]},
    {"id":"OP03","name":"Pillars of Strength","slug":"one-piece-pillars-of-strength","release_date":"2023-06-01","msrp":100.0,"type":"main","products":["booster-box"]},
    {"id":"OP04","name":"Kingdoms of Intrigue","slug":"one-piece-kingdoms-of-intrigue","release_date":"2023-09-22","msrp":100.0,"type":"main","products":["booster-box"]},
    {"id":"OP05","name":"Awakening of the New Era","slug":"one-piece-awakening-of-the-new-era","release_date":"2023-12-08","msrp":100.0,"type":"main","products":["booster-box"]},
    {"id":"OP06","name":"Wings of the Captain","slug":"one-piece-wings-of-the-captain","release_date":"2024-03-08","msrp":100.0,"type":"main","products":["booster-box"]},
    {"id":"OP07","name":"500 Years in the Future","slug":"one-piece-500-years-in-the-future","release_date":"2024-06-28","msrp":100.0,"type":"main","products":["booster-box"]},
    {"id":"OP08","name":"Two Legends","slug":"one-piece-two-legends","release_date":"2024-09-06","msrp":100.0,"type":"main","products":["booster-box"]},
    {"id":"OP09","name":"Emperors in the New World","slug":"one-piece-emperors-in-the-new-world","release_date":"2024-12-13","msrp":120.0,"type":"main","products":["booster-box"]},
    {"id":"OP10","name":"Royal Blood","slug":"one-piece-royal-blood","release_date":"2025-03-07","msrp":120.0,"type":"main","products":["booster-box"]},
    {"id":"OP11","name":"Fist of Divine Speed","slug":"one-piece-fist-of-divine-speed","release_date":"2025-06-01","msrp":120.0,"type":"main","products":["booster-box"]},
    {"id":"OP12","name":"Legacy of the Master","slug":"one-piece-legacy-of-the-master","release_date":"2025-08-01","msrp":120.0,"type":"main","products":["booster-box"]},
    {"id":"OP13","name":"Carrying On His Will","slug":"one-piece-carrying-on-his-will","release_date":"2025-11-01","msrp":120.0,"type":"main","products":["booster-box"]},
    {"id":"OP14","name":"Azure Sea's Seven","slug":"one-piece-azure-seas-seven","release_date":"2026-01-16","msrp":120.0,"type":"main","products":["booster-box"]},
    {"id":"OP15","name":"Adventure on Kami's Island","slug":"one-piece-adventure-on-kamis-island","release_date":"2026-04-03","msrp":120.0,"type":"main","products":["booster-box"]},
    {"id":"EB01","name":"Extra Booster Memorial Collection","slug":"one-piece-extra-booster-memorial-collection","release_date":"2024-01-26","msrp":70.0,"type":"extra","products":["sealed-booster-box"]},
    {"id":"EB02","name":"Extra Booster Anime 25th Collection","slug":"one-piece-extra-booster-anime-25th-collection","release_date":"2025-05-01","msrp":70.0,"type":"extra","products":["booster-box"]},
    {"id":"EB03","name":"Extra Booster Heroines Edition","slug":"one-piece-extra-booster-heroines-edition","release_date":"2026-02-20","msrp":70.0,"type":"extra","products":["booster-box"]},
    {"id":"PRB01","name":"Premium Booster (Card the Best)","slug":"one-piece-premium-booster","release_date":"2024-11-08","msrp":120.0,"type":"premium","products":["premium-booster-display"]},
    {"id":"PRB02","name":"Premium Booster 2","slug":"one-piece-premium-booster-2","release_date":"2025-11-01","msrp":120.0,"type":"premium","products":["booster-box"]},
]

def fetch(set_slug, product_slug, retries=3):
    url = f"https://www.pricecharting.com/game/{set_slug}/{product_slug}"
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=25)
            r.encoding = "utf-8"
        except requests.exceptions.ConnectionError:
            if attempt < retries-1:
                w = (attempt+1)*8 + random.uniform(3,6)
                print(f"    ↻ Retrying in {w:.1f}s ({attempt+1}/{retries})")
                time.sleep(w)
                continue
            return None
        except requests.RequestException as e:
            print(f"    ✗ {e}"); return None
        if r.status_code == 404: return None
        if r.status_code == 429:
            time.sleep(30+random.uniform(5,15)); continue
        if r.status_code != 200:
            print(f"    ✗ HTTP {r.status_code}"); return None
        html = r.text
        dm = re.search(r'VGPC\.chart_data\s*=\s*(\{.*?\});\s*\n', html, re.DOTALL)
        if dm:
            try:
                cd = json.loads(dm.group(1))
                for k in ["used","complete","new","boxonly","loose"]:
                    if k in cd and cd[k]: return cd[k]
            except: pass
        lm = re.search(r'VGPC\.chart_data\s*=\s*(\[.*?\]);', html, re.DOTALL)
        if lm:
            try: return json.loads(lm.group(1))
            except: pass
        return None
    return None

def parse(data):
    out = []
    for p in data:
        try:
            ts, cents = float(p[0]), float(p[1])
            if cents == 0: continue
            dt = datetime.fromtimestamp(ts/1000, tz=timezone.utc)
            out.append((dt.strftime("%Y-%m-%d"), round(cents/100, 2)))
        except: continue
    return out

def run():
    print("\n" + "="*54)
    print("  One Piece TCG — Box Price Scraper")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*54 + "\n")

    keys, rows = set(), []
    if os.path.exists(OUTPUT):
        with open(OUTPUT, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                rows.append(row)
                keys.add((row["date"], row["set_id"], row["product"]))
        print(f"  → Loaded {len(rows)} existing rows\n")

    print("  Warming up (this takes ~20s)...")
    try:
        SESSION.get("https://www.pricecharting.com/", timeout=20)
        time.sleep(6+random.uniform(2,4))
        SESSION.get("https://www.pricecharting.com/category/one-piece-card-game", timeout=20)
        time.sleep(6+random.uniform(2,4))
        print("  ✓ Ready\n")
    except: print("  ⚠ Warmup failed — continuing anyway\n")

    new_rows, total = [], 0

    for s_idx, s in enumerate(SETS):
        if s_idx > 0 and s_idx % 5 == 0:
            pause = 30 + random.uniform(10, 20)
            print(f"  ── Resting {pause:.0f}s ({s_idx}/{len(SETS)} sets done) ──\n")
            time.sleep(pause)
        print(f"[{s['id']}] {s['name']}")
        for product in s["products"]:
            label = product.replace("-"," ").title()
            print(f"  {label}")
            data = fetch(s["slug"], product)
            if data is None:
                print(f"    ✗ No data"); time.sleep(2); continue
            added = 0
            for date_str, price in parse(data):
                key = (date_str, s["id"], label)
                if key in keys: continue
                new_rows.append({"date":date_str,"set_id":s["id"],"set_name":s["name"],
                    "product":label,"price_usd":price,"msrp":s["msrp"],"set_type":s["type"]})
                keys.add(key); added += 1
            total += added
            print(f"    ✓ {added} new points")
            time.sleep(5.0+random.uniform(0,3.0))
        print()

    if new_rows:
        all_rows = sorted(rows+new_rows, key=lambda r:(r["date"],r["set_id"],r["product"]))
        with open(OUTPUT,"w",newline="",encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=HEADERS_CSV)
            w.writeheader(); w.writerows(all_rows)
        print(f"  ✅ {total} new points added → {len(all_rows)} total rows")
        print(f"  💾 {os.path.abspath(OUTPUT)}")
    else:
        print("  ✅ No new data — already up to date")
    print("="*54+"\n")

if __name__ == "__main__":
    run()
