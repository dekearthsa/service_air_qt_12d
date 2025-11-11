import sqlite3

DB_PATH = "sensor_data_projectD.db"
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# cursor.execute(""" DROP TABLE sensor_data """)
data =cursor.execute(""" SELECT DISTINCT sensor_type FROM sensor_data   """).fetchall()
print(data)


## 'Temp before filter',), ('Diff pressure',), ('Fan speed',), ('Duct temperature',), ('Duct humidity',), ('Duct co2',), ('Duct voc',) ('CO2',), ('Temperature',), ('Humidity',), ('Voltage',), ('RSSI',)