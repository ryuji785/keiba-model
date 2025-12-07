import sqlite3
conn=sqlite3.connect('data/keiba.db')
print(conn.execute('PRAGMA table_info(trainers)').fetchall())
