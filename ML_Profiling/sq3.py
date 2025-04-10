import sqlite3

DB_NAME = "driver.db"
DRIVER_TABLE_NAME = "driver"
USER_TABLE_NAME = "user"

# Connect to SQLite (Modify for MySQL/PostgreSQL)
conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {DRIVER_TABLE_NAME} (
    driver_id TEXT PRIMARY KEY,
    safety_rating REAL,
    cleanliness_rating REAL,
    punctuality REAL,
    cancelled_ride REAL,
    overall_rating REAL
)
""")

cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {USER_TABLE_NAME} (
    user_id TEXT PRIMARY KEY,
    trust_rating REAL,
    cleanliness_rating REAL,
    punctuality REAL,
    cancelled_ride REAL,
    overall_rating REAL
)
""")

conn.commit()
conn.close()