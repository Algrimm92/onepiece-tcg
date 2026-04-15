"""
One Piece TCG — Card Price Scraper B (OP10–PRB02)
===================================================
Scrapes apex card prices for sets OP10 through PRB02.
Run monthly after scraper_cards_a.py. Takes ~5 minutes.
Wait 15+ minutes after running scraper_cards_a.py first.

Usage:  python scraper_cards_b.py
Output: one_piece_cards.csv
"""
import csv, json, re, os, sys, time, random, unicodedata
from datetime import datetime, timezone
try:
    from curl_cffi import requests
    CURL_MODE = True
except ImportError:
    import requests
    CURL_MODE = False

os.chdir(os.path.expanduser("~/TCG/onepiece/exports"))

OUTPUT = "one_piece_cards.csv"
HEADERS_CSV = ["date","set_id","set_name","card_name","card_code","rarity_tier","price_usd","psa10_est_lo","psa10_est_hi"]

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
    print("  Using standard requests (install curl-cffi for better bypass)")

PSA_MULT = {
    "Red MR":(1.5,2.0),"Gold MR":(2.0,2.5),"Gold SP":(1.5,2.0),
    "Silver SP":(1.5,2.0),"MR":(2.0,2.5),"SP":(1.8,2.3),
    "SP Foil":(1.8,2.3),"TR":(2.0,2.5),"Alt Art":(1.5,2.0),
    "Leader Alt":(1.5,2.0),"Pre-Errata":(6.0,8.4),"Manga Event":(1.5,2.0),
}

CARDS = [
    {"set_id":"OP10","set_name":"Royal Blood","card_name":"Trafalgar Law [Manga] OP10-119","card_code":"OP10-119","rarity_tier":"MR","pc_set_slug":"one-piece-royal-blood"},
    {"set_id":"OP10","set_name":"Royal Blood","card_name":"Divine Departure [Alternate Art] OP10-019","card_code":"OP10-019","rarity_tier":"Manga Event","pc_set_slug":"one-piece-royal-blood"},
    {"set_id":"OP11","set_name":"Fist of Divine Speed","card_name":"Monkey.D.Luffy [SP Gold] OP05-119","card_code":"OP05-119","rarity_tier":"Gold SP","pc_set_slug":"one-piece-fist-of-divine-speed"},
    {"set_id":"OP11","set_name":"Fist of Divine Speed","card_name":"Monkey.D.Luffy [SP Silver] OP05-119","card_code":"OP05-119","rarity_tier":"Silver SP","pc_set_slug":"one-piece-fist-of-divine-speed"},
    {"set_id":"OP12","set_name":"Legacy of the Master","card_name":"Marshall.D.Teach [SP Gold] OP09-093","card_code":"OP09-093","rarity_tier":"Gold SP","pc_set_slug":"one-piece-legacy-of-the-master"},
    {"set_id":"OP12","set_name":"Legacy of the Master","card_name":"Jewelry Bonney [Manga] OP12-118","card_code":"OP12-118","rarity_tier":"MR","pc_set_slug":"one-piece-legacy-of-the-master"},
    {"set_id":"OP13","set_name":"Carrying On His Will","card_name":"Monkey.D.Luffy [Red Manga] OP13-118","card_code":"OP13-118","rarity_tier":"Red MR","pc_set_slug":"one-piece-carrying-on-his-will"},
    {"set_id":"OP13","set_name":"Carrying On His Will","card_name":"Sabo [Red Manga] OP13-120","card_code":"OP13-120","rarity_tier":"Red MR","pc_set_slug":"one-piece-carrying-on-his-will"},
    {"set_id":"OP13","set_name":"Carrying On His Will","card_name":"Shanks [Gold] OP09-004","card_code":"OP09-004","rarity_tier":"Gold SP","pc_set_slug":"one-piece-carrying-on-his-will"},
    {"set_id":"OP14","set_name":"Azure Sea's Seven","card_name":"Buggy [SP Gold] OP09-051","card_code":"OP09-051","rarity_tier":"Gold SP","pc_set_slug":"one-piece-azure-sea's-seven"},
    {"set_id":"OP14","set_name":"Azure Sea's Seven","card_name":"Dracule Mihawk [Manga] OP14-119","card_code":"OP14-119","rarity_tier":"MR","pc_set_slug":"one-piece-azure-sea's-seven"},
    {"set_id":"EB01","set_name":"Extra Booster Memorial Collection","card_name":"Tony Tony.Chopper [Alternate Art Manga] EB01-006","card_code":"EB01-006","rarity_tier":"MR","pc_set_slug":"one-piece-extra-booster-memorial-collection"},
    {"set_id":"EB02","set_name":"Extra Booster Anime 25th Collection","card_name":"Monkey.D.Luffy [Manga] EB02-061","card_code":"EB02-061","rarity_tier":"MR","pc_set_slug":"one-piece-extra-booster-anime-25th-collection"},
    {"set_id":"EB02","set_name":"Extra Booster Anime 25th Collection","card_name":"Nami [Special Alternate Art] OP01-016","card_code":"OP01-016","rarity_tier":"SP","pc_set_slug":"one-piece-romance-dawn"},
    {"set_id":"EB03","set_name":"Extra Booster Heroines Edition","card_name":"Nami [SP] EB03-053","card_code":"EB03-053","rarity_tier":"SP","pc_set_slug":"one-piece-extra-booster-heroines-edition"},
    {"set_id":"EB03","set_name":"Extra Booster Heroines Edition","card_name":"Nico Robin [SP] EB03-055","card_code":"EB03-055","rarity_tier":"SP","pc_set_slug":"one-piece-extra-booster-heroines-edition"},
    {"set_id":"PRB01","set_name":"Premium Booster (Card the Best)","card_name":"Monkey.D.Luffy [Manga PRB01] OP05-119","card_code":"OP05-119","rarity_tier":"MR","pc_set_slug":"one-piece-awakening-of-the-new-era"},
    {"set_id":"PRB01","set_name":"Premium Booster (Card the Best)","card_name":"Tony Tony.Chopper [Manga PRB01] EB01-006","card_code":"EB01-006","rarity_tier":"MR","pc_set_slug":"one-piece-extra-booster-memorial-collection"},
    {"set_id":"PRB02","set_name":"Premium Booster 2","card_name":"Sanji [Manga] OP06-119","card_code":"OP06-119","rarity_tier":"MR","pc_set_slug":"one-piece-premium-booster-2"},
    {"set_id":"PRB02","set_name":"Premium Booster 2","card_name":"Gum-Gum Giant [Alternate Art PRB-02] OP09-078","card_code":"OP09-078","rarity_tier":"Manga Event","pc_set_slug":"one-piece-premium-booster-2"},
]

def make_slug(name):
    s = name.lower()
    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode()
    s = re.sub(r'\.', '', s)
    s = re.sub(r'[\[\]\s]+', '-', s)
    s = re.sub(r'[^a-z0-9-]', '', s)
    s = re.sub(r'-+', '-', s)
    return s.strip('-')

def fetch(set_slug, card_slug, retries=3):
    url = f"https://www.pricecharting.com/game/{set_slug}/{card_slug}"
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=25)
            r.encoding = "utf-8"
        except requests.exceptions.ConnectionError:
            if attempt < retries-1:
                w = (attempt+1)*5 + random.uniform(2,4)
                print(f"    ↻ Retrying in {w:.1f}s ({attempt+1}/{retries})")
                time.sleep(w); continue
            return None, url
        except requests.RequestException as e:
            print(f"    ✗ {e}"); return None, url
        if r.status_code == 404: return None, url
        if r.status_code == 429:
            time.sleep(45+random.uniform(10,20)); continue
        if r.status_code != 200:
            print(f"    ✗ HTTP {r.status_code}"); return None, url
        html = r.text
        dm = re.search(r'VGPC\.chart_data\s*=\s*(\{.*?\});\s*\n', html, re.DOTALL)
        if dm:
            try:
                cd = json.loads(dm.group(1))
                for k in ["used","complete","new","boxonly","loose"]:
                    if k in cd and cd[k]: return cd[k], url
            except: pass
        lm = re.search(r'VGPC\.chart_data\s*=\s*(\[.*?\]);', html, re.DOTALL)
        if lm:
            try: return json.loads(lm.group(1)), url
            except: pass
        return None, url
    return None, url

def parse(data):
    out = []
    for p in data:
        try:
            ts, cents = float(p[0]), float(p[1])
            if cents == 0: continue
            dt = datetime.fromtimestamp(ts/1000, tz=timezone.utc)
            out.append((dt.strftime("%Y-%m-%d"), round(cents/100,2)))
        except: continue
    return out

def run():
    print("\n" + "="*54)
    print("  One Piece TCG — Card Scraper B (OP10–PRB02)")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  {len(CARDS)} cards to scrape")
    print("="*54+"\n")

    keys, rows = set(), []
    if os.path.exists(OUTPUT):
        with open(OUTPUT, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                rows.append(row)
                keys.add((row["date"],row["set_id"],row["card_name"]))
        print(f"  → Loaded {len(rows)} existing rows\n")

    print("  Warming up...")
    try:
        SESSION.get("https://www.pricecharting.com/", timeout=20)
        time.sleep(4+random.uniform(1,3))
        print("  ✓ Ready\n")
    except: print("  ⚠ Warmup failed\n")

    new_rows, total, failed = [], 0, []

    for i, card in enumerate(CARDS):
        if i > 0 and i % 8 == 0:
            pause = 25+random.uniform(5,15)
            print(f"\n  ── Break {pause:.0f}s ({i}/{len(CARDS)} done) ──\n")
            time.sleep(pause)

        slug = make_slug(card["card_name"])
        print(f"  [{card['set_id']}] {card['card_name'][:55]}")
        data, url = fetch(card["pc_set_slug"], slug)

        if data is None:
            failed.append((card["set_id"], card["card_name"], url))
            print(f"    ✗ Not found")
            time.sleep(3+random.uniform(0,2))
            continue

        lo, hi = PSA_MULT.get(card["rarity_tier"], (1.5,2.0))
        added = 0
        for date_str, price in parse(data):
            key = (date_str, card["set_id"], card["card_name"])
            if key in keys: continue
            new_rows.append({
                "date":date_str,"set_id":card["set_id"],"set_name":card["set_name"],
                "card_name":card["card_name"],"card_code":card["card_code"],
                "rarity_tier":card["rarity_tier"],"price_usd":price,
                "psa10_est_lo":round(price*lo,2),"psa10_est_hi":round(price*hi,2)
            })
            keys.add(key); added += 1
        total += added
        print(f"    ✓ {added} new points")
        time.sleep(4.5+random.uniform(0,3))

    if new_rows:
        all_rows = sorted(rows+new_rows, key=lambda r:(r["date"],r["set_id"],r["card_name"]))
        with open(OUTPUT,"w",newline="",encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=HEADERS_CSV)
            w.writeheader(); w.writerows(all_rows)
        print(f"\n  ✅ {total} new points → {len(all_rows)} total rows")
        print(f"  💾 {os.path.abspath(OUTPUT)}")
    else:
        print("\n  ✅ Already up to date")

    if failed:
        print(f"\n  ⚠ {len(failed)} cards not found:")
        for sid, name, url in failed:
            print(f"    [{sid}] {name[:60]}")
            print(f"           {url}")
    print("="*54+"\n")

if __name__ == "__main__":
    run()
