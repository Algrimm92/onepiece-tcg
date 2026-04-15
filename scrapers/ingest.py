import requests
import sqlite3
from datetime import date

# --- Setup Database ---
conn = sqlite3.connect('/Users/alexgrimm/TCG/onepiece/data/onepiece.db')
cursor = conn.cursor()

# Create tables
cursor.execute('''
    CREATE TABLE IF NOT EXISTS cards (
        card_set_id TEXT PRIMARY KEY,
        card_name TEXT,
        set_name TEXT,
        set_id TEXT,
        rarity TEXT,
        card_color TEXT,
        card_type TEXT,
        card_cost TEXT,
        card_power TEXT,
        counter_amount INTEGER,
        attribute TEXT,
        sub_types TEXT,
        card_text TEXT,
        card_image TEXT
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        card_set_id TEXT,
        market_price REAL,
        inventory_price REAL,
        date_scraped TEXT,
        date_pulled TEXT
    )
''')

conn.commit()

# --- Fetch Data ---
print("Fetching cards from OPTCGAPI...")
response = requests.get('https://optcgapi.com/api/allSetCards/')
cards = response.json()
print(f"Got {len(cards)} cards")

today = str(date.today())

# --- Insert Data ---
for card in cards:
    cursor.execute('''
        INSERT OR IGNORE INTO cards VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ''', (
        card['card_set_id'],
        card['card_name'],
        card['set_name'],
        card['set_id'],
        card['rarity'],
        card['card_color'],
        card['card_type'],
        card.get('card_cost'),
        card.get('card_power'),
        card.get('counter_amount'),
        card.get('attribute'),
        card.get('sub_types'),
        card.get('card_text'),
        card.get('card_image')
    ))

    cursor.execute('''
        INSERT INTO prices (card_set_id, market_price, inventory_price, date_scraped, date_pulled)
        VALUES (?,?,?,?,?)
    ''', (
        card['card_set_id'],
        card.get('market_price'),
        card.get('inventory_price'),
        card.get('date_scraped', today),
        today
    ))

conn.commit()
conn.close()
print("Done. Database saved as onepiece.db")
