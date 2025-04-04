import psycopg2
import sqlite3
import time
import json
import queue

start_time = time.time()

tx_queue = queue.Queue()

# replace the following values with the ones provided in Demeter's console
connection = psycopg2.connect(
    host="dbsync-v3.demeter.run",
    port=5432,
    user="", # your username
    password="", # your password
    dbname="dbsync-mainnet",
)

# Create a cursor object
cursor = connection.cursor()

query = """
SELECT tx.id, tx.block_id, tx.block_index, tx_metadata.json
FROM tx
JOIN tx_metadata 
  ON tx_metadata.tx_id = tx.id
    AND tx_metadata.key = '21325'
ORDER BY tx.block_id ASC, tx.block_index ASC
"""

# Execute a query
#cursor.execute("SELECT t.* FROM public.tx_metadata t WHERE key = '21325' ORDER BY id DESC LIMIT 5")
cursor.execute(query)

# Fetch and print the results
rows = cursor.fetchall()

for row in rows:
    transformed_row = (row[0], row[1], row[2], json.dumps(row[3]))
    tx_queue.put(transformed_row)

fetch_end_time = time.time()
fetch_duration = fetch_end_time - start_time
print(f"Data fetched from the database in {fetch_duration} seconds. {tx_queue.qsize()} transactions fetched.")

sqlite_conn = sqlite3.connect("tx.sqlite")
sqlite_cursor = sqlite_conn.cursor()

sqlite_cursor.execute("""
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY,
    block_id INTEGER,
    block_index INTEGER,
    metadata_json TEXT
)
""")

rows_to_insert = []
while not tx_queue.empty():
    rows_to_insert.append(tx_queue.get())

sqlite_cursor.executemany("""
INSERT INTO transactions (id, block_id, block_index, metadata_json)
VALUES (?, ?, ?, ?)
""", rows_to_insert)

# Commit changes and close connections
sqlite_conn.commit()

# Check the count of transactions in the SQLite database
sqlite_cursor.execute("SELECT COUNT(*) FROM transactions")
tx_count = sqlite_cursor.fetchone()[0]

sqlite_conn.close()
cursor.close()
connection.close()

end_time = time.time()
duration = end_time - start_time

print(f"Data successfully inserted into local SQLite database.")
print(f"Duration of the process: {duration} seconds")
print(f"Total number of transactions in tx.db: {tx_count}")