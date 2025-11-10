import sqlite3

DB_PATH = "sensor_data_projectD.db"
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# cursor.execute(""" DROP TABLE sensor_data """)
data =cursor.execute(""" SELECT * FROM sensor_data WHERE timestamp BETWEEN  1761824991000 AND 1761954587000  """).fetchall()
print(data)