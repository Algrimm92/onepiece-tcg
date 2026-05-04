"""
tcgp_ingest.py
================
TCGplayer pricing ingestion for One Piece Card Game via tcgcsv.com.

Mirrors the cumulative architecture of ingest.py:
  - reads existing SQLite, never overwrites
  - appends a daily price snapshot per (product, sub_type)
  - upserts catalog (groups/products) so new sets are picked up automatically

Usage (macOS Terminal, from ~/TCG/onepiece/):
    python3 scrapers/tcgp_ingest.py

Tables created (separate from existing OPTCGAPI tables):
    tcgp_groups   - One Piece sets (e.g. OP01 Romance Dawn)
    tcgp_products - Cards AND sealed products (booster boxes, starter decks)
    tcgp_prices   - Daily snapshots, composite PK prevents duplicates

Note: tcgcsv.com requires a custom User-Agent header. Default python-requests
agents are blocked with 401 Unauthorized.
"""

import sqlite3
import requests
import time
from datetime import datetime, timezone

# ---------------------------------------------------------------- Config
ONE_PIECE_CATEGORY_ID = 68
TCGCSV_BASE = "https://tcgcsv.com/tcgplayer"
DB_PATH = "data/onepiece.db"   # relative to ~/TCG/onepiece/ (run from project root)
REQUEST_DELAY = 0.4             # be polite — tcgcsv is a free Patreon-funded service

# tcgcsv.com requires a custom User-Agent or it will return 401 Unauthorized.
# Format: ApplicationName/Version
USER_AGENT = "OnePieceTCGTracker/0.1.0"


# ---------------------------------------------------------------- DB
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_schema(conn):
    """Create tcgp_* tables if they don't exist. Idempotent."""
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS tcgp_groups (
            group_id      INTEGER PRIMARY KEY,
            name          TEXT,
            abbreviation  TEXT,
            published_on  TEXT,
            modified_on   TEXT,
            category_id   INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS tcgp_products (
            product_id    INTEGER PRIMARY KEY,
            group_id      INTEGER,
            name          TEXT,
            clean_name    TEXT,
            url           TEXT,
            modified_on   TEXT,
            category_id   INTEGER,
            number        TEXT,
            rarity        TEXT,
            card_type     TEXT,
            is_sealed     INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS tcgp_prices (
            product_id        INTEGER,
            sub_type_name     TEXT,
            snapshot_date     TEXT,
            low_price         REAL,
            mid_price         REAL,
            high_price        REAL,
            market_price      REAL,
            direct_low_price  REAL,
            PRIMARY KEY (product_id, sub_type_name, snapshot_date)
        )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_prices_date ON tcgp_prices(snapshot_date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_prices_pid  ON tcgp_prices(product_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_sealed ON tcgp_products(is_sealed)")

    conn.commit()


# ---------------------------------------------------------------- HTTP
# Reuse a session so the User-Agent header is sent on every request
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})


def fetch_json(url):
    """Polite fetcher — sends the required custom User-Agent header."""
    time.sleep(REQUEST_DELAY)
    print(f"  GET {url}")
    r = SESSION.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------- Helpers
def extract_extended(extended_data, key):
    """Pull a value from TCGplayer's extendedData array of {name, displayName, value}."""
    if not extended_data:
        return None
    for item in extended_data:
        if item.get("name") == key:
            return item.get("value")
    return None


def is_sealed_product(name):
    """Heuristic: classify as sealed product based on name patterns."""
    if not name:
        return 0
    name_lower = name.lower()
    sealed_keywords = [
        "booster box", "booster pack", "starter deck", "ultra deck",
        "case", "display", "bundle", "tin", "premium booster",
        "double pack", "gift collection", "gift set",
    ]
    return 1 if any(kw in name_lower for kw in sealed_keywords) else 0


# ---------------------------------------------------------------- Ingestion
def ingest_groups(conn):
    """Pull all One Piece groups (sets) and upsert. Returns list of group_ids."""
    print("[1/3] Fetching groups (sets)...")
    data = fetch_json(f"{TCGCSV_BASE}/{ONE_PIECE_CATEGORY_ID}/groups")
    groups = data.get("results", [])

    cur = conn.cursor()
    for g in groups:
        cur.execute("""
            INSERT INTO tcgp_groups
                (group_id, name, abbreviation, published_on, modified_on, category_id)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(group_id) DO UPDATE SET
                name         = excluded.name,
                abbreviation = excluded.abbreviation,
                published_on = excluded.published_on,
                modified_on  = excluded.modified_on
        """, (
            g.get("groupId"),
            g.get("name"),
            g.get("abbreviation"),
            g.get("publishedOn"),
            g.get("modifiedOn"),
            ONE_PIECE_CATEGORY_ID,
        ))
    conn.commit()
    print(f"  Stored {len(groups)} groups")
    return [g["groupId"] for g in groups]


def ingest_products(conn, group_ids):
    """Pull products for each group and upsert. Catalog data, mostly static."""
    print(f"[2/3] Fetching products for {len(group_ids)} groups...")
    cur = conn.cursor()
    total = 0

    for gid in group_ids:
        try:
            data = fetch_json(f"{TCGCSV_BASE}/{ONE_PIECE_CATEGORY_ID}/{gid}/products")
            products = data.get("results", [])
        except requests.HTTPError as e:
            print(f"  Skipping group {gid}: {e}")
            continue

        for p in products:
            ext = p.get("extendedData", [])
            cur.execute("""
                INSERT INTO tcgp_products
                    (product_id, group_id, name, clean_name, url, modified_on,
                     category_id, number, rarity, card_type, is_sealed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(product_id) DO UPDATE SET
                    name        = excluded.name,
                    clean_name  = excluded.clean_name,
                    url         = excluded.url,
                    modified_on = excluded.modified_on,
                    number      = excluded.number,
                    rarity      = excluded.rarity,
                    card_type   = excluded.card_type,
                    is_sealed   = excluded.is_sealed
            """, (
                p.get("productId"),
                gid,
                p.get("name"),
                p.get("cleanName"),
                p.get("url"),
                p.get("modifiedOn"),
                ONE_PIECE_CATEGORY_ID,
                extract_extended(ext, "Number"),
                extract_extended(ext, "Rarity"),
                extract_extended(ext, "CardType"),
                is_sealed_product(p.get("name")),
            ))
        total += len(products)

    conn.commit()
    print(f"  Stored/updated {total} products")


def ingest_prices(conn, group_ids):
    """Pull current prices for each group and append today's snapshot."""
    print(f"[3/3] Fetching prices for {len(group_ids)} groups...")
    snapshot_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cur = conn.cursor()
    total = 0
    new_records = 0

    for gid in group_ids:
        try:
            data = fetch_json(f"{TCGCSV_BASE}/{ONE_PIECE_CATEGORY_ID}/{gid}/prices")
            prices = data.get("results", [])
        except requests.HTTPError as e:
            print(f"  Skipping group {gid}: {e}")
            continue

        for p in prices:
            try:
                cur.execute("""
                    INSERT OR IGNORE INTO tcgp_prices
                        (product_id, sub_type_name, snapshot_date,
                         low_price, mid_price, high_price,
                         market_price, direct_low_price)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    p.get("productId"),
                    p.get("subTypeName") or "",
                    snapshot_date,
                    p.get("lowPrice"),
                    p.get("midPrice"),
                    p.get("highPrice"),
                    p.get("marketPrice"),
                    p.get("directLowPrice"),
                ))
                if cur.rowcount > 0:
                    new_records += 1
            except Exception as e:
                print(f"  Error on product {p.get('productId')}: {e}")
        total += len(prices)

    conn.commit()
    print(f"  Processed {total} price records, {new_records} NEW for {snapshot_date}")


# ---------------------------------------------------------------- Main
def main():
    started = datetime.now()
    print(f"=== TCGplayer/One Piece ingest started at {started} ===")

    conn = get_db()
    init_schema(conn)

    group_ids = ingest_groups(conn)
    ingest_products(conn, group_ids)
    ingest_prices(conn, group_ids)

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM tcgp_groups")
    g = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM tcgp_products")
    p_total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM tcgp_products WHERE is_sealed=1")
    p_sealed = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT snapshot_date) FROM tcgp_prices")
    d = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM tcgp_prices")
    pr = cur.fetchone()[0]
    cur.execute("SELECT MIN(snapshot_date), MAX(snapshot_date) FROM tcgp_prices")
    dmin, dmax = cur.fetchone()

    elapsed = (datetime.now() - started).total_seconds()
    print(f"\n=== Summary ===")
    print(f"Groups (sets):              {g}")
    print(f"Products total:             {p_total}")
    print(f"  of which sealed:          {p_sealed}")
    print(f"Snapshot dates accumulated: {d}  ({dmin} to {dmax})")
    print(f"Total price records:        {pr}")
    print(f"Elapsed:                    {elapsed:.1f}s")
    conn.close()


if __name__ == "__main__":
    main()
