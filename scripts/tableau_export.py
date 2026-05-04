"""
tableau_export.py
=================
Export purpose-built CSVs from onepiece.db for Tableau Public dashboards
and SQL practice.

Each export is a real SQL query you wrote — useful as resume material AND as
the foundation for a Tableau viz. CSVs land in exports/tableau/.

Usage (from ~/TCG/onepiece/):
    python3 scripts/tableau_export.py

Run this AFTER tcgp_ingest.py so the latest snapshot is included.
"""

import sqlite3
import csv
import os
from datetime import datetime

DB_PATH = "data/onepiece.db"
EXPORT_DIR = "exports/tableau"


# ---------------------------------------------------------------- Queries
# Each query is named — the name becomes the CSV filename.
# Built for Tableau ingestion: flat tables, no nested data, sensible column names.
QUERIES = {

    # ---- 1. Sealed product master list with current prices ----
    # Use case: "what does the sealed universe look like right now?"
    # Tableau: bar charts ranked by market price, filterable by set
    "sealed_current_prices": """
        SELECT
            p.product_id,
            p.name                 AS product_name,
            g.name                 AS set_name,
            g.abbreviation         AS set_code,
            p.rarity,
            pr.snapshot_date,
            pr.sub_type_name       AS printing,
            pr.market_price,
            pr.low_price,
            pr.mid_price,
            pr.high_price,
            pr.direct_low_price,
            ROUND(pr.market_price - pr.low_price, 2)                   AS market_low_spread,
            ROUND((pr.market_price - pr.low_price) / pr.market_price * 100, 1)
                                                                       AS market_low_spread_pct
        FROM tcgp_products p
        JOIN tcgp_groups   g  ON g.group_id = p.group_id
        JOIN tcgp_prices   pr ON pr.product_id = p.product_id
        WHERE p.is_sealed = 1
          AND pr.snapshot_date = (SELECT MAX(snapshot_date) FROM tcgp_prices)
          AND pr.market_price IS NOT NULL
        ORDER BY pr.market_price DESC
    """,

    # ---- 2. Full sealed price history (longitudinal) ----
    # Use case: time-series trend charts
    # Tableau: line charts of market_price over snapshot_date, by product
    # Note: will grow over time as snapshots accumulate — that's the point
    "sealed_price_history": """
        SELECT
            p.product_id,
            p.name           AS product_name,
            g.abbreviation   AS set_code,
            pr.snapshot_date,
            pr.sub_type_name AS printing,
            pr.market_price,
            pr.low_price
        FROM tcgp_products p
        JOIN tcgp_groups   g  ON g.group_id = p.group_id
        JOIN tcgp_prices   pr ON pr.product_id = p.product_id
        WHERE p.is_sealed = 1
          AND pr.market_price IS NOT NULL
        ORDER BY p.product_id, pr.snapshot_date
    """,

    # ---- 3. Set-level summary ----
    # Use case: "which sets are most valuable in aggregate?"
    # Tableau: KPI cards, set comparison bar charts
    "set_summary": """
        SELECT
            g.group_id,
            g.name                     AS set_name,
            g.abbreviation             AS set_code,
            g.published_on,
            COUNT(DISTINCT p.product_id)                          AS total_products,
            SUM(CASE WHEN p.is_sealed = 1 THEN 1 ELSE 0 END)      AS sealed_products,
            SUM(CASE WHEN p.is_sealed = 0 THEN 1 ELSE 0 END)      AS singles_products,
            ROUND(AVG(CASE WHEN p.is_sealed = 1 THEN pr.market_price END), 2)
                                                                  AS avg_sealed_market_price,
            ROUND(MAX(CASE WHEN p.is_sealed = 1 THEN pr.market_price END), 2)
                                                                  AS max_sealed_market_price,
            ROUND(SUM(CASE WHEN p.is_sealed = 1 THEN pr.market_price END), 2)
                                                                  AS total_sealed_market_value
        FROM tcgp_groups   g
        JOIN tcgp_products p  ON p.group_id = g.group_id
        LEFT JOIN tcgp_prices pr
                              ON pr.product_id = p.product_id
                             AND pr.snapshot_date = (SELECT MAX(snapshot_date) FROM tcgp_prices)
        GROUP BY g.group_id, g.name, g.abbreviation, g.published_on
        ORDER BY total_sealed_market_value DESC
    """,

    # ---- 4. Top singles (chase cards) by set ----
    # Use case: identify which singles drive each set's heat
    # Tableau: top-N tables by set, scatter of rarity vs price
    "top_singles_by_set": """
        SELECT
            p.product_id,
            p.name           AS card_name,
            g.abbreviation   AS set_code,
            p.number         AS card_number,
            p.rarity,
            p.card_type,
            pr.sub_type_name AS printing,
            pr.market_price,
            pr.low_price,
            pr.snapshot_date
        FROM tcgp_products p
        JOIN tcgp_groups   g  ON g.group_id = p.group_id
        JOIN tcgp_prices   pr ON pr.product_id = p.product_id
        WHERE p.is_sealed = 0
          AND pr.snapshot_date = (SELECT MAX(snapshot_date) FROM tcgp_prices)
          AND pr.market_price IS NOT NULL
          AND pr.market_price >= 5.0
        ORDER BY g.abbreviation, pr.market_price DESC
    """,

    # ---- 5. Booster Box only — your investment focus ----
    # Use case: pure investment vehicle tracking
    # Tableau: the centerpiece of your portfolio dashboard
    "booster_boxes_only": """
        SELECT
            p.product_id,
            p.name              AS product_name,
            g.name              AS set_name,
            g.abbreviation      AS set_code,
            g.published_on      AS set_release_date,
            pr.snapshot_date,
            pr.market_price,
            pr.low_price,
            pr.high_price,
            ROUND(julianday(pr.snapshot_date) - julianday(g.published_on))
                                AS days_since_release
        FROM tcgp_products p
        JOIN tcgp_groups   g  ON g.group_id = p.group_id
        JOIN tcgp_prices   pr ON pr.product_id = p.product_id
        WHERE p.is_sealed = 1
          AND LOWER(p.name) LIKE '%booster box%'
          AND LOWER(p.name) NOT LIKE '%case%'
          AND pr.market_price IS NOT NULL
        ORDER BY pr.snapshot_date DESC, pr.market_price DESC
    """,

    # ---- 6. Sealed product taxonomy ----
    # Use case: understand the sealed product mix per set
    # Tableau: stacked bar charts showing product type breakdown
    "sealed_taxonomy": """
        SELECT
            g.abbreviation AS set_code,
            CASE
                WHEN LOWER(p.name) LIKE '%booster box case%'           THEN 'Booster Box Case'
                WHEN LOWER(p.name) LIKE '%booster box%'                THEN 'Booster Box'
                WHEN LOWER(p.name) LIKE '%booster pack%'               THEN 'Booster Pack'
                WHEN LOWER(p.name) LIKE '%starter deck%'               THEN 'Starter Deck'
                WHEN LOWER(p.name) LIKE '%ultra deck%'                 THEN 'Ultra Deck'
                WHEN LOWER(p.name) LIKE '%premium booster%'            THEN 'Premium Booster'
                WHEN LOWER(p.name) LIKE '%display%'                    THEN 'Display'
                WHEN LOWER(p.name) LIKE '%bundle%'                     THEN 'Bundle'
                WHEN LOWER(p.name) LIKE '%tin%'                        THEN 'Tin'
                WHEN LOWER(p.name) LIKE '%gift%'                       THEN 'Gift Set'
                ELSE                                                        'Other Sealed'
            END AS product_type,
            COUNT(*)                                                   AS product_count,
            ROUND(AVG(pr.market_price), 2)                             AS avg_market_price,
            ROUND(MIN(pr.market_price), 2)                             AS min_market_price,
            ROUND(MAX(pr.market_price), 2)                             AS max_market_price
        FROM tcgp_products p
        JOIN tcgp_groups   g  ON g.group_id = p.group_id
        JOIN tcgp_prices   pr ON pr.product_id = p.product_id
        WHERE p.is_sealed = 1
          AND pr.snapshot_date = (SELECT MAX(snapshot_date) FROM tcgp_prices)
          AND pr.market_price IS NOT NULL
        GROUP BY g.abbreviation, product_type
        ORDER BY g.abbreviation, avg_market_price DESC
    """,
}


# ---------------------------------------------------------------- Export
def export_query(conn, name, sql):
    """Run a query and write results to exports/tableau/{name}.csv"""
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    headers = [d[0] for d in cur.description]

    out_path = os.path.join(EXPORT_DIR, f"{name}.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(rows)

    return len(rows), out_path


def main():
    started = datetime.now()
    print(f"=== Tableau export started at {started} ===")

    if not os.path.exists(DB_PATH):
        print(f"ERROR: Database not found at {DB_PATH}")
        print("Run scrapers/tcgp_ingest.py first to populate the DB.")
        return

    os.makedirs(EXPORT_DIR, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    print(f"\nWriting CSVs to: {EXPORT_DIR}/\n")

    total_rows = 0
    for name, sql in QUERIES.items():
        try:
            rows, path = export_query(conn, name, sql)
            print(f"  ✓ {name:30} {rows:>6} rows")
            total_rows += rows
        except Exception as e:
            print(f"  ✗ {name:30} FAILED: {e}")

    conn.close()

    elapsed = (datetime.now() - started).total_seconds()
    print(f"\n=== Done — {total_rows} total rows across {len(QUERIES)} CSVs in {elapsed:.1f}s ===")


if __name__ == "__main__":
    main()
