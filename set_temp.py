import sqlite3
import os

os.makedirs("F:/Temp", exist_ok=True)

con = sqlite3.connect("data/db/patents.db")
con.execute("PRAGMA temp_store = 1")
con.execute("PRAGMA temp_store_directory = 'F:/Temp'")
con.close()
print("Done - temp set to F:/Temp")